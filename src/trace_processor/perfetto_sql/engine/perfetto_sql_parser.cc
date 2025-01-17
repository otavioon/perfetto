/*
 * Copyright (C) 2023 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "src/trace_processor/perfetto_sql/engine/perfetto_sql_parser.h"

#include <algorithm>

#include "perfetto/base/logging.h"
#include "perfetto/base/status.h"
#include "perfetto/ext/base/string_utils.h"
#include "src/trace_processor/sqlite/sql_source.h"
#include "src/trace_processor/sqlite/sqlite_tokenizer.h"

namespace perfetto {
namespace trace_processor {
namespace {

using Token = SqliteTokenizer::Token;
using Statement = PerfettoSqlParser::Statement;

enum class State {
  kStmtStart,
  kCreate,
  kCreatePerfetto,
  kPassthrough,
};

bool KeywordEqual(std::string_view expected, std::string_view actual) {
  PERFETTO_DCHECK(std::all_of(expected.begin(), expected.end(), islower));
  return std::equal(expected.begin(), expected.end(), actual.begin(),
                    actual.end(),
                    [](char a, char b) { return a == tolower(b); });
}

bool TokenIsSqliteKeyword(std::string_view keyword, SqliteTokenizer::Token t) {
  return t.token_type == SqliteTokenType::TK_GENERIC_KEYWORD &&
         KeywordEqual(keyword, t.str);
}

bool TokenIsCustomKeyword(std::string_view keyword, SqliteTokenizer::Token t) {
  return t.token_type == SqliteTokenType::TK_ID && KeywordEqual(keyword, t.str);
}

bool TokenIsTerminal(Token t) {
  return t.token_type == SqliteTokenType::TK_SEMI || t.str.empty();
}

}  // namespace

PerfettoSqlParser::PerfettoSqlParser(SqlSource sql)
    : sql_(std::move(sql)), tokenizer_(sql_.sql().c_str()) {}

bool PerfettoSqlParser::Next() {
  PERFETTO_DCHECK(status_.ok());

  State state = State::kStmtStart;
  const char* non_space_ptr = nullptr;
  for (Token token = tokenizer_.Next();; token = tokenizer_.Next()) {
    // Space should always be completely ignored by any logic below as it will
    // never change the current state in the state machine.
    if (token.token_type == SqliteTokenType::TK_SPACE) {
      continue;
    }

    if (TokenIsTerminal(token)) {
      // If we have a non-space character we've seen, just return all the stuff
      // after that point.
      if (non_space_ptr) {
        uint32_t offset_of_non_space =
            static_cast<uint32_t>(non_space_ptr - sql_.sql().c_str());
        uint32_t chars_since_non_space =
            static_cast<uint32_t>(tokenizer_.ptr() - non_space_ptr);
        statement_ =
            SqliteSql{sql_.Substr(offset_of_non_space, chars_since_non_space)};
        return true;
      }
      // This means we've seen a semi-colon without any non-space content. Just
      // try and find the next statement as this "statement" is a noop.
      if (token.token_type == SqliteTokenType::TK_SEMI) {
        continue;
      }
      // This means we've reached the end of the SQL.
      PERFETTO_DCHECK(token.str.empty());
      return false;
    }

    // If we've not seen a space character, keep track of the current position.
    if (!non_space_ptr) {
      non_space_ptr = token.str.data();
    }

    switch (state) {
      case State::kPassthrough:
        break;
      case State::kStmtStart:
        state = TokenIsSqliteKeyword("create", token) ? State::kCreate
                                                      : State::kPassthrough;
        break;
      case State::kCreate:
        if (TokenIsSqliteKeyword("trigger", token)) {
          // TODO(lalitm): add this to the "errors" documentation page
          // explaining why this is the case.
          base::StackString<1024> err(
              "Creating triggers are not supported by trace processor.");
          return ErrorAtToken(token, err.c_str());
        }
        state = TokenIsCustomKeyword("perfetto", token) ? State::kCreatePerfetto
                                                        : State::kPassthrough;
        break;
      case State::kCreatePerfetto:
        if (TokenIsCustomKeyword("function", token)) {
          return ParseCreatePerfettoFunction();
        }
        base::StackString<1024> err(
            "Expected 'table' after 'create perfetto', received "
            "%*s.",
            static_cast<int>(token.str.size()), token.str.data());
        return ErrorAtToken(token, err.c_str());
    }
  }
}

bool PerfettoSqlParser::ParseCreatePerfettoFunction() {
  std::string prototype;
  Token function_name = tokenizer_.NextNonWhitespace();
  if (function_name.token_type != SqliteTokenType::TK_ID) {
    // TODO(lalitm): add a link to create function documentation.
    base::StackString<1024> err("Invalid function name %.*s",
                                static_cast<int>(function_name.str.size()),
                                function_name.str.data());
    return ErrorAtToken(function_name, err.c_str());
  }
  prototype.append(function_name.str);

  // TK_LP == '(' (i.e. left parenthesis).
  Token lp = tokenizer_.NextNonWhitespace();
  if (lp.token_type != SqliteTokenType::TK_LP) {
    // TODO(lalitm): add a link to create function documentation.
    return ErrorAtToken(lp, "Malformed function prototype: '(' expected");
  }
  prototype.append(lp.str);

  for (Token tok = tokenizer_.Next();; tok = tokenizer_.Next()) {
    if (tok.token_type == SqliteTokenType::TK_SPACE) {
      prototype.append(" ");
      continue;
    }
    prototype.append(tok.str);
    if (tok.token_type == SqliteTokenType::TK_ID ||
        tok.token_type == SqliteTokenType::TK_COMMA) {
      continue;
    }
    if (tok.token_type == SqliteTokenType::TK_RP) {
      break;
    }
    // TODO(lalitm): add a link to create function documentation.
    return ErrorAtToken(
        tok, "Malformed function prototype: ')', ',', name or type expected");
  }

  if (Token returns = tokenizer_.NextNonWhitespace();
      !TokenIsCustomKeyword("returns", returns)) {
    // TODO(lalitm): add a link to create function documentation.
    return ErrorAtToken(returns, "Expected keyword 'returns'");
  }

  Token ret_token = tokenizer_.NextNonWhitespace();
  if (ret_token.token_type != SqliteTokenType::TK_ID) {
    // TODO(lalitm): add a link to create function documentation.
    return ErrorAtToken(ret_token, "Invalid return type");
  }

  if (Token as_token = tokenizer_.NextNonWhitespace();
      !TokenIsSqliteKeyword("as", as_token)) {
    // TODO(lalitm): add a link to create function documentation.
    return ErrorAtToken(as_token, "Expected keyword 'as'");
  }

  Token first = tokenizer_.NextNonWhitespace();
  Token token = first;
  for (; !TokenIsTerminal(token); token = tokenizer_.Next()) {
  }
  uint32_t offset = static_cast<uint32_t>(first.str.data() - sql_.sql().data());
  uint32_t len = static_cast<uint32_t>(token.str.end() - sql_.sql().data());

  statement_ = CreateFunction{std::move(prototype), std::string(ret_token.str),
                              sql_.Substr(offset, len)};
  return true;
}

bool PerfettoSqlParser::ErrorAtToken(const SqliteTokenizer::Token& token,
                                     const char* error) {
  uint32_t offset = static_cast<uint32_t>(token.str.data() - sql_.sql().data());
  std::string traceback = sql_.AsTracebackFrame(offset);
  status_ = base::ErrStatus("%s%s", traceback.c_str(), error);
  return false;
}

}  // namespace trace_processor
}  // namespace perfetto

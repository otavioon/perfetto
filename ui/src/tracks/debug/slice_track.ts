// Copyright (C) 2023 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import m from 'mithril';

import {Actions, DEBUG_SLICE_TRACK_KIND} from '../../common/actions';
import {EngineProxy} from '../../common/engine';
import {globals} from '../../frontend/globals';
import {
  NamedSliceTrackTypes,
} from '../../frontend/named_slice_track';
import {NewTrackArgs} from '../../frontend/track';
import {TrackButton, TrackButtonAttrs} from '../../frontend/track_panel';
import {
  CustomSqlDetailsPanelConfig,
  CustomSqlTableDefConfig,
  CustomSqlTableSliceTrack,
} from '../custom_sql_table_slices';

import {ARG_PREFIX} from './add_debug_track_menu';
import {DebugSliceDetailsTab} from './details_tab';

// Names of the columns of the underlying view to be used as ts / dur / name.
export interface SliceColumns {
  ts: string;
  dur: string;
  name: string;
}

export interface DebugTrackV2Config {
  sqlTableName: string;
  columns: SliceColumns;
}

interface DebugTrackV2Types extends NamedSliceTrackTypes {
  config: DebugTrackV2Config;
}

export class DebugTrackV2 extends CustomSqlTableSliceTrack<DebugTrackV2Types> {
  static readonly kind = DEBUG_SLICE_TRACK_KIND;

  static create(args: NewTrackArgs) {
    return new DebugTrackV2(args);
  }

  constructor(args: NewTrackArgs) {
    super(args);
  }

  getSqlDataSource(): CustomSqlTableDefConfig {
    return {
      sqlTableName: this.config.sqlTableName,
    };
  }

  getDetailsPanel(): CustomSqlDetailsPanelConfig {
    return {
      kind: DebugSliceDetailsTab.kind,
      config: {
        sqlTableName: this.config.sqlTableName,
        title: 'Debug Slice',
      },
    };
  }

  async initSqlTable(tableName: string): Promise<void> {
    super.initSqlTable(tableName);
  }

  getTrackShellButtons(): Array<m.Vnode<TrackButtonAttrs>> {
    return [m(TrackButton, {
      action: () => {
        globals.dispatch(Actions.removeDebugTrack({trackId: this.trackId}));
      },
      i: 'close',
      tooltip: 'Close',
      showButton: true,
    })];
  }
}

let debugTrackCount = 0;

export async function addDebugTrack(
    engine: EngineProxy,
    sqlViewName: string,
    trackName: string,
    sliceColumns: SliceColumns,
    argColumns: string[]) {
  // QueryResultTab has successfully created a view corresponding to |uuid|.
  // To prepare displaying it as a track, we materialize it and compute depths.
  const debugTrackId = ++debugTrackCount;
  const sqlTableName = `materialized_${debugTrackId}_${sqlViewName}`;
  // TODO(altimin): Support removing this table when the track is closed.
  const dur = sliceColumns.dur === '0' ? 0 : sliceColumns.dur;
  await engine.query(`
      create table ${sqlTableName} as
      with prepared_data as (
        select
          row_number() over () as id,
          ${sliceColumns.ts} as ts,
          ifnull(cast(${dur} as int), -1) as dur,
          printf('%s', ${sliceColumns.name}) as name
          ${argColumns.length > 0 ? ',' : ''}
          ${argColumns.map((c) => `${c} as ${ARG_PREFIX}${c}`).join(',\n')}
        from ${sqlViewName}
      )
      select
        *
      from prepared_data
      order by ts;`);

  globals.dispatch(Actions.addDebugTrack({
    engineId: engine.engineId,
    name: trackName.trim() || `Debug Track ${debugTrackId}`,
    config: {
      sqlTableName,
      columns: sliceColumns,
    },
  }));
}

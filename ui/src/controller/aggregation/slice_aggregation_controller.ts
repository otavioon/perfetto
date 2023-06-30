// Copyright (C) 2020 The Android Open Source Project
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

import {ColumnDef} from '../../common/aggregation_data';
import {Engine} from '../../common/engine';
import {Area, Sorting} from '../../common/state';
import {globals} from '../../frontend/globals';
import {
  ASYNC_SLICE_TRACK_KIND,
  Config as AsyncSliceConfig,
} from '../../tracks/async_slices';
import {
  Config as SliceConfig,
  SLICE_TRACK_KIND,
} from '../../tracks/chrome_slices';

import {AggregationController} from './aggregation_controller';

export function getSelectedTrackIds(area: Area): number[] {
  const selectedTrackIds = [];
  for (const trackId of area.tracks) {
    const track = globals.state.tracks[trackId];
    // Track will be undefined for track groups.
    if (track !== undefined) {
      if (track.kind === SLICE_TRACK_KIND) {
        selectedTrackIds.push((track.config as SliceConfig).trackId);
      }
      if (track.kind === ASYNC_SLICE_TRACK_KIND) {
        const config = track.config as AsyncSliceConfig;
        for (const id of config.trackIds) {
          selectedTrackIds.push(id);
        }
      }
    }
  }
  return selectedTrackIds;
}

export class SliceAggregationController extends AggregationController {
  async createAggregateView(engine: Engine, area: Area) {
    await engine.query(`drop view if exists ${this.kind};`);

    const selectedTrackIds = getSelectedTrackIds(area);

    if (selectedTrackIds.length === 0) return false;

    const query = `create view ${this.kind} as
        SELECT
        name,
        sum(dur) AS total_dur,
        sum(dur)/count(1) as avg_dur,
        count(1) as occurrences
        FROM slices
        WHERE track_id IN (${selectedTrackIds}) AND
        ts + dur > ${area.start} AND
        ts < ${area.end} group by name`;

    await engine.query(query);
    return true;
  }

  getTabName() {
    return 'Slices';
  }

  async getExtra() {}

  getDefaultSorting(): Sorting {
    return {column: 'total_dur', direction: 'DESC'};
  }

  getColumnDefinitions(): ColumnDef[] {
    return [
      {
        title: 'Name',
        kind: 'STRING',
        columnConstructor: Uint32Array,
        columnId: 'name',
      },
      {
        title: 'Wall duration (ms)',
        kind: 'TIMESTAMP_NS',
        columnConstructor: Float64Array,
        columnId: 'total_dur',
        sum: true,
      },
      {
        title: 'Avg Wall duration (ms)',
        kind: 'TIMESTAMP_NS',
        columnConstructor: Float64Array,
        columnId: 'avg_dur',
      },
      {
        title: 'Occurrences',
        kind: 'NUMBER',
        columnConstructor: Uint32Array,
        columnId: 'occurrences',
        sum: true,
      },
      {
        title: 'Occurrences2 bla',
        kind: 'NUMBER',
        columnConstructor: Uint32Array,
        columnId: 'occurrences',
        sum: true,
      },
    ];
  }
}


export class SliceAggregationController2 extends AggregationController {
  async createAggregateView(engine: Engine, area: Area) {
    await engine.query(`drop view if exists ${this.kind};`);
    await engine.query(`drop view if exists selection_area;`);

    const selectedTrackIds = getSelectedTrackIds(area);

    if (selectedTrackIds.length === 0) return false;

    const total_dur = area.end - area.start
    const total_duration_seconds = Number(total_dur) / 1e6
    
    // TODO 3 queries: idle, data amount, and processing / data
    const query = `
    CREATE VIEW ${this.kind} AS
    SELECT
        track_id,
        ROUND(SUM(dur) / 1e9, 4) AS track_processing_time,
        COUNT(1) AS num_occurrences,
        ROUND(((SUM(CASE WHEN category LIKE "%processing%" THEN dur ELSE 0 END) / 1e6) / ${total_duration_seconds}) * 100, 2) AS track_percentage_processing,
        ROUND(((SUM(CASE WHEN category LIKE "%transfering%" THEN dur ELSE 0 END) / 1e6) / ${total_duration_seconds}) * 100, 2) AS track_percentage_transfering,
        CASE WHEN ((SUM(CASE WHEN category LIKE "%processing%" THEN dur ELSE 0 END) / 1e6) / ${total_duration_seconds}) > 0.8 THEN 
          'X' ELSE 
          'Possible causes: (1) Small chunk size. Try increasing the chunk size; (2) Small kernel. Try increasing the kernel complexity by adding more operations; (3) Bad partitioning.' 
          END AS hint,
        COUNT(1) / 0 as track_data_amount
    FROM
        slices
    WHERE
        track_id IN (${selectedTrackIds}) AND
        ts + dur > ${area.start} AND
        ts < ${area.end}
    GROUP BY
        track_id;
    `
    await engine.query(query);

    const query2 = `create view selection_area as
      SELECT *
      FROM slices
      WHERE track_id IN (${selectedTrackIds}) AND 
      ts + dur > ${area.start} AND
      ts < ${area.end}
    `
    await engine.query(query2);
    return true;
  }

  getTabName() {
    return 'Performance';
  }

  async getExtra() {}

  getDefaultSorting(): Sorting {
    return {column: 'track_id', direction: 'ASC'};
  }

  getColumnDefinitions(): ColumnDef[] {
    return [
      {
        title: 'Track',
        kind: 'NUMBER',
        columnConstructor: Uint32Array,
        columnId: 'track_id',
      },
      {
        title: 'Number of elements',
        kind: 'NUMBER',
        columnConstructor: Uint32Array,
        columnId: 'num_occurrences',
      },
      {
        title: 'Percentage of total processing time (%)',
        kind: 'NUMBER',
        columnConstructor: Float64Array,
        columnId: 'track_percentage_processing',
      },
      {
        title: 'Percentage of total transfering time (%)',
        kind: 'NUMBER',
        columnConstructor: Float64Array,
        columnId: 'track_percentage_transfering',
      },
      {
        title: 'Total amount of data used (MB)',
        kind: 'NUMBER',
        columnConstructor: Uint32Array,
        columnId: 'track_data_amount',
      },
      {
        title: 'Processing time / byte',
        kind: 'NUMBER',
        columnConstructor: Uint32Array,
        columnId: 'track_data_amount',
      },
      {
        title: 'Hint',
        kind: 'STRING',
        columnConstructor: Uint32Array,
        columnId: 'hint',
      },
    ];
  }
}

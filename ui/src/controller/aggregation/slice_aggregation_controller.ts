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
import {toNs} from '../../common/time';
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
        ts + dur > ${toNs(area.startSec)} AND
        ts < ${toNs(area.endSec)} group by name`;

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

    const total_dur = area.endSec - area.startSec

    const query = `create view ${this.kind} as
        SELECT
        track_id AS track_id, 
        SUM(dur)/1e6 AS track_processing_time, 
        AVG(dur)/1e6 AS track_average_processing, 
        COUNT(1) AS num_occurrences,
        (SUM(CASE WHEN category like "%processing%" THEN dur ELSE 0 END)/${total_dur})/1e9 AS track_percentage_processing,
        (SUM(CASE WHEN category like "%transfering%" THEN dur ELSE 0 END)/${total_dur})/1e9 AS track_percentage_transfering
        FROM slices
        WHERE track_id IN (${selectedTrackIds}) AND 
        ts + dur > ${toNs(area.startSec)} AND
        ts < ${toNs(area.endSec)}
        GROUP BY track_id
    `
    await engine.query(query);

    const query2 = `create view selection_area as
      SELECT *
      FROM slices
      WHERE track_id IN (${selectedTrackIds}) AND 
      ts + dur > ${toNs(area.startSec)} AND
      ts < ${toNs(area.endSec)}
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
        title: 'Processing time (ms)',
        kind: 'NUMBER',
        columnConstructor: Float64Array,
        columnId: 'track_processing_time',
      },
      {
        title: 'Avg Processing time (ms)',
        kind: 'NUMBER',
        columnConstructor: Float64Array,
        columnId: 'track_average_processing',
      },
      {
        title: 'Occurrences',
        kind: 'NUMBER',
        columnConstructor: Uint32Array,
        columnId: 'num_occurrences',
      },
      {
        title: 'Percentage of total processing time',
        kind: 'NUMBER',
        columnConstructor: Float64Array,
        columnId: 'track_percentage_processing',
      },
      {
        title: 'Percentage of total transfering time',
        kind: 'NUMBER',
        columnConstructor: Float64Array,
        columnId: 'track_percentage_transfering',
      },
    ];
  }
}

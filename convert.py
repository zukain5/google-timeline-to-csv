import argparse
import json
import pathlib
import sys
import traceback

import pandas as pd

ACTIVITY_COLS = [
    'timeline_type',
    'start_latitude',
    'start_longitude',
    'end_latitude',
    'end_longitude',
    'start_time',
    'end_time',
    'distance',
    'activity_type',
]

VISIT_COLS = [
    'timeline_type',
    'location_latitude',
    'location_longitude',
    'place_id',
    'address',
    'name',
    'start_time',
    'end_time',
]


def load_activity(timeline_obj):
    timeline_type = 'activity'
    activity = timeline_obj['activitySegment']
    start_lat = activity['startLocation'].get('latitudeE7', None)
    start_lon = activity['startLocation'].get('longitudeE7', None)
    end_lat = activity['endLocation'].get('latitudeE7', None)
    end_lon = activity['endLocation'].get('longitudeE7', None)
    start_time = activity['duration'].get('startTimestamp', None)
    end_time = activity['duration'].get('endTimestamp', None)
    distance = activity.get('distance', None)
    activity_type = activity.get('activityType', None)

    return pd.DataFrame(
        [[
            timeline_type,
            start_lat,
            start_lon,
            end_lat,
            end_lon,
            start_time,
            end_time,
            distance,
            activity_type,
        ]],
        columns=ACTIVITY_COLS,
    )


def load_visit(timeline_obj):
    timeline_type = 'visit'
    visit = timeline_obj['placeVisit']
    location = visit['location']
    location_lat = location.get('latitudeE7', None)
    location_lon = location.get('longitudeE7', None)
    place_id = location.get('placeId', None)
    address = location.get('address', None)
    name = location.get('name', None)
    start_time = visit['duration'].get('startTimestamp', None)
    end_time = visit['duration'].get('endTimestamp', None)

    return pd.DataFrame(
        [[
            timeline_type,
            location_lat,
            location_lon,
            place_id,
            address,
            name,
            start_time,
            end_time,
        ]],
        columns=VISIT_COLS,
    )


def load_timeline_obj(timeline_obj):
    try:
        if len(timeline_obj) > 1:
            raise KeyError(
                '''
                Key number in a timeline object is expected to be 1,
                but there are more than 1 key in the timeline object.
                '''
            )

        if 'activitySegment' in timeline_obj.keys():
            timeline_row = load_activity(timeline_obj)
        elif 'placeVisit' in timeline_obj.keys():
            timeline_row = load_visit(timeline_obj)
        else:
            raise KeyError(
                '''
                Unexpected key exists in the timeline object.
                Please contact to the developer.
                '''
            )
    except KeyError as e:
        print(e)
        traceback.print_exc()
        sys.exit(1)

    return timeline_row


def load_monthly_json(path):
    with open(path, 'r') as f:
        location_history = json.load(f)

    activity_df = pd.DataFrame([], columns=ACTIVITY_COLS)
    visit_df = pd.DataFrame([], columns=VISIT_COLS)

    for timeline_obj in location_history['timelineObjects']:
        timeline_row = load_timeline_obj(timeline_obj)

        match timeline_row.loc[0, 'timeline_type']:
            case 'activity':
                activity_df = pd.concat([activity_df, timeline_row], axis=0, ignore_index=True)
            case 'visit':
                visit_df = pd.concat([visit_df, timeline_row], axis=0, ignore_index=True)

    return {
        'activity': activity_df,
        'visit': visit_df,
    }


def main():
    parser = argparse.ArgumentParser(
        description='Convert Google timeline data to csv.'
    )

    parser.add_argument('input', help='input folder (Semantic Location History)')
    parser.add_argument('output', help='''
        output directory path (not file path because the program outputs mutiple files)
    ''')

    args = parser.parse_args()

    activity_df = pd.DataFrame([], columns=ACTIVITY_COLS)
    visit_df = pd.DataFrame([], columns=VISIT_COLS)

    input_dir = pathlib.Path(args.input)

    for monthly_json in input_dir.glob('**/*.json'):
        monthly_data = load_monthly_json(monthly_json)
        activity_df = pd.concat([activity_df, monthly_data['activity']], axis=0, ignore_index=True)
        visit_df = pd.concat([visit_df, monthly_data['visit']], axis=0, ignore_index=True)

    activity_df = activity_df.sort_values('start_time')
    activity_df = activity_df.reset_index(drop=True)
    activity_df.to_csv(f'{args.output}/activity.csv')

    visit_df = visit_df.sort_values('start_time')
    visit_df = visit_df.reset_index(drop=True)
    visit_df.to_csv(f'{args.output}/visit.csv')


if __name__ == '__main__':
    main()

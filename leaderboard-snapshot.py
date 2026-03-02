import json
import os
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime, timezone
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
cloudwatch = boto3.client('cloudwatch')
scores_table = dynamodb.Table('leaderboard-scores')
snapshots_table = dynamodb.Table('leaderboard-snapshots')

# Optional: publish to SNS when new #1 is detected
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN', '')
sns = boto3.client('sns') if SNS_TOPIC_ARN else None

CORS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'POST,GET,OPTIONS',
    'Content-Type': 'application/json',
}


def compute_analytics(items):
    """Compute aggregate metrics from the full leaderboard."""
    if not items:
        return {}

    scores = [int(it.get('score', 0)) for it in items]
    scores_sorted = sorted(scores, reverse=True)
    n = len(scores_sorted)

    # Percentile boundaries
    p50 = scores_sorted[n // 2] if n > 0 else 0
    p90 = scores_sorted[n // 10] if n >= 10 else scores_sorted[0]
    p99 = scores_sorted[n // 100] if n >= 100 else scores_sorted[0]

    # Country breakdown
    countries = {}
    for it in items:
        c = it.get('country', 'XX')
        countries[c] = countries.get(c, 0) + 1

    # Games played distribution
    total_games = sum(int(it.get('games_played', 0)) for it in items)
    avg_games = round(total_games / n, 1) if n > 0 else 0

    return {
        'total_players': n,
        'highest_score': scores_sorted[0] if scores_sorted else 0,
        'lowest_score': scores_sorted[-1] if scores_sorted else 0,
        'average_score': round(sum(scores) / n) if n > 0 else 0,
        'median_score': p50,
        'p90_score': p90,
        'p99_score': p99,
        'score_spread': scores_sorted[0] - scores_sorted[-1] if n > 1 else 0,
        'total_games_played': total_games,
        'avg_games_per_player': avg_games,
        'country_breakdown': countries,
        'top_country': max(countries, key=countries.get) if countries else 'N/A',
    }


def take_snapshot(leaderboard_id, top_n=50):
    now = datetime.now(timezone.utc)
    now_iso = now.strftime('%Y-%m-%dT%H:%M:%SZ')
    snapshot_id = f'{leaderboard_id}#{now_iso}'

    # Query top-N for the snapshot
    resp = scores_table.query(
        IndexName='leaderboard-rank-index',
        KeyConditionExpression=Key('leaderboard_id').eq(leaderboard_id),
        ScanIndexForward=True,
        Limit=top_n,
    )
    top_items = resp.get('Items', [])

    # Query ALL players for analytics (paginate if large)
    all_items = []
    kwargs = {
        'IndexName': 'leaderboard-rank-index',
        'KeyConditionExpression': Key('leaderboard_id').eq(leaderboard_id),
    }
    while True:
        page = scores_table.query(**kwargs)
        all_items.extend(page.get('Items', []))
        if 'LastEvaluatedKey' not in page or len(all_items) > 5000:
            break
        kwargs['ExclusiveStartKey'] = page['LastEvaluatedKey']

    analytics = compute_analytics(all_items)

    # Write rank 0 = analytics summary row
    snapshots_table.put_item(Item={
        'snapshot_id': snapshot_id,
        'rank': 0,
        'record_type': 'analytics',
        'snapshot_at': now_iso,
        'leaderboard_id': leaderboard_id,
        **{k: v if not isinstance(v, float) else Decimal(str(v))
           for k, v in analytics.items()
           if k != 'country_breakdown'},
        'country_breakdown': json.dumps(analytics.get('country_breakdown', {})),
    })

    # Write individual rank rows
    written = 0
    for rank, item in enumerate(top_items, start=1):
        snapshots_table.put_item(Item={
            'snapshot_id': snapshot_id,
            'rank': rank,
            'player_id': item.get('player_id'),
            'display_name': item.get('display_name', ''),
            'score': int(item.get('score', 0)),
            'games_played': int(item.get('games_played', 0)),
            'country': item.get('country', ''),
            'snapshot_at': now_iso,
        })
        written += 1

    # Publish CloudWatch custom metrics
    metric_data = [
        {'MetricName': 'TotalPlayers', 'Value': analytics.get('total_players', 0),
         'Unit': 'Count', 'Dimensions': [
            {'Name': 'LeaderboardId', 'Value': leaderboard_id}]},
        {'MetricName': 'HighestScore', 'Value': analytics.get('highest_score', 0),
         'Unit': 'Count', 'Dimensions': [
            {'Name': 'LeaderboardId', 'Value': leaderboard_id}]},
        {'MetricName': 'AverageScore', 'Value': analytics.get('average_score', 0),
         'Unit': 'Count', 'Dimensions': [
            {'Name': 'LeaderboardId', 'Value': leaderboard_id}]},
        {'MetricName': 'TotalGamesPlayed', 'Value': analytics.get('total_games_played', 0),
         'Unit': 'Count', 'Dimensions': [
            {'Name': 'LeaderboardId', 'Value': leaderboard_id}]},
    ]
    cloudwatch.put_metric_data(
        Namespace='LeaderboardSystem', MetricData=metric_data)

    # Notify SNS if new #1 detected
    if SNS_TOPIC_ARN and sns and top_items:
        leader = top_items[0]
        try:
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=f'Leaderboard Snapshot — {leaderboard_id}',
                Message=json.dumps({
                    'event': 'snapshot_taken',
                    'leaderboard_id': leaderboard_id,
                    'snapshot_id': snapshot_id,
                    'leader': leader.get('display_name', ''),
                    'leader_score': int(leader.get('score', 0)),
                    'total_players': analytics.get('total_players', 0),
                    'average_score': analytics.get('average_score', 0),
                }, default=str),
            )
        except Exception:
            pass

    return {
        'snapshot_id': snapshot_id,
        'players_saved': written,
        'analytics': analytics,
    }


def lambda_handler(event, context):
    # Handle API Gateway calls
    if event.get('httpMethod'):
        if event['httpMethod'] == 'OPTIONS':
            return {'statusCode': 200, 'headers': CORS, 'body': ''}
        body = json.loads(event.get('body', '{}'))
        lb_id = body.get('leaderboard_id', 'all-time')
        top_n = min(int(body.get('top_n', 50)), 100)
    # Handle EventBridge Scheduler calls (no httpMethod)
    else:
        lb_id = event.get('leaderboard_id', 'all-time')
        top_n = int(event.get('top_n', 50))

    result = take_snapshot(lb_id, top_n)

    return {
        'statusCode': 200,
        'headers': CORS,
        'body': json.dumps(result, default=str),
    }

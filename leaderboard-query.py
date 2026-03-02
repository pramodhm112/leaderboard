import json
import time
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime, timezone

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('leaderboard-scores')

CORS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'POST,GET,OPTIONS',
    'Content-Type': 'application/json',
}


def get_top_n(leaderboard_id, limit=25):
    """Query GSI for top-N players. Ascending inverted = descending real score."""
    start = time.time()

    resp = table.query(
        IndexName='leaderboard-rank-index',
        KeyConditionExpression=Key('leaderboard_id').eq(leaderboard_id),
        ScanIndexForward=True,   # ascending inverted = descending real
        Limit=limit,
    )

    players = []
    for rank, item in enumerate(resp.get('Items', []), start=1):
        players.append({
            'rank': rank,
            'player_id': item.get('player_id', ''),
            'display_name': item.get('display_name', ''),
            'score': int(item.get('score', 0)),
            'avatar_url': item.get('avatar_url', ''),
            'games_played': int(item.get('games_played', 0)),
            'country': item.get('country', ''),
            'last_score_at': item.get('last_score_at', ''),
        })

    elapsed = round((time.time() - start) * 1000)
    return {
        'leaderboard_id': leaderboard_id,
        'players': players,
        'count': len(players),
        'query_time_ms': elapsed,
    }


def get_total_players(leaderboard_id):
    """Count total players in a leaderboard (for percentile denominator)."""
    resp = table.query(
        IndexName='leaderboard-rank-index',
        KeyConditionExpression=Key('leaderboard_id').eq(leaderboard_id),
        Select='COUNT',
    )
    return resp.get('Count', 0)


def lambda_handler(event, context):
    if event.get('httpMethod') == 'OPTIONS':
        return {'statusCode': 200, 'headers': CORS, 'body': ''}

    params = event.get('queryStringParameters') or {}
    now = datetime.now(timezone.utc)

    period = params.get('period', 'all-time')
    if period == 'daily':
        lb_id = f'daily-{now.strftime("%Y-%m-%d")}'
    elif period == 'weekly':
        lb_id = f'weekly-{now.strftime("%Y-W%W")}'
    else:
        lb_id = 'all-time'

    limit = min(int(params.get('limit', '25')), 100)

    result = get_top_n(lb_id, limit)
    result['total_players'] = get_total_players(lb_id)

    return {
        'statusCode': 200,
        'headers': CORS,
        'body': json.dumps(result, default=str),
    }

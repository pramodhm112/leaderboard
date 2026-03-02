import json
import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('leaderboard-scores')

CORS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'POST,GET,OPTIONS',
    'Content-Type': 'application/json',
}


def get_player_stats(player_id):
    """Get a player's rank and percentile across all leaderboards."""
    # Query base table: get all leaderboard entries for this player
    resp = table.query(
        KeyConditionExpression=Key('player_id').eq(player_id)
    )
    items = resp.get('Items', [])

    if not items:
        return {'player_id': player_id, 'found': False}

    leaderboards = {}
    for item in items:
        lb_id = item['leaderboard_id']
        score = int(item.get('score', 0))
        inv_score = item.get('inverted_score', '')

        # Count players with LOWER inverted_score (= HIGHER real score) → rank
        rank_resp = table.query(
            IndexName='leaderboard-rank-index',
            KeyConditionExpression=(
                Key('leaderboard_id').eq(lb_id) &
                Key('inverted_score').lt(inv_score)
            ),
            Select='COUNT',
        )
        rank = rank_resp.get('Count', 0) + 1

        # Total players for percentile
        total_resp = table.query(
            IndexName='leaderboard-rank-index',
            KeyConditionExpression=Key('leaderboard_id').eq(lb_id),
            Select='COUNT',
        )
        total = max(total_resp.get('Count', 1), 1)
        percentile = round(((total - rank) / total) * 100, 1)

        leaderboards[lb_id] = {
            'score': score,
            'rank': rank,
            'total_players': total,
            'percentile': percentile,
            'games_played': int(item.get('games_played', 0)),
        }

    primary = items[0]
    return {
        'player_id': player_id,
        'found': True,
        'display_name': primary.get('display_name', ''),
        'avatar_url': primary.get('avatar_url', ''),
        'country': primary.get('country', ''),
        'leaderboards': leaderboards,
    }


def lambda_handler(event, context):
    if event.get('httpMethod') == 'OPTIONS':
        return {'statusCode': 200, 'headers': CORS, 'body': ''}

    params = event.get('queryStringParameters') or {}
    pid = params.get('player_id', '').strip().lower()

    if not pid:
        return {
            'statusCode': 400,
            'headers': CORS,
            'body': json.dumps({'error': 'player_id query param is required'}),
        }

    stats = get_player_stats(pid)
    return {
        'statusCode': 200,
        'headers': CORS,
        'body': json.dumps(stats, default=str),
    }

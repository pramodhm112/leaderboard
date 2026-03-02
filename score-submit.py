import json
import boto3
from datetime import datetime, timezone

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('leaderboard-scores')

MAX_SCORE = 999999
SCORE_PAD = 7

CORS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'POST,GET,OPTIONS',
    'Content-Type': 'application/json',
}


def make_inverted(score, player_id):
    """Convert real score to inverted sort key for descending order in GSI."""
    inv = MAX_SCORE - int(score)
    return f'{str(inv).zfill(SCORE_PAD)}#{player_id}'


def get_leaderboard_ids():
    """Return all active leaderboard period keys."""
    now = datetime.now(timezone.utc)
    return [
        'all-time',
        f'daily-{now.strftime("%Y-%m-%d")}',
        f'weekly-{now.strftime("%Y-W%W")}',
    ]


def submit_score(player_id, score, display_name, country='US'):
    """Submit a score across all leaderboard periods. High-score-wins logic."""
    now_iso = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    results = []

    for lb_id in get_leaderboard_ids():
        # Read current score (if any)
        existing = table.get_item(
            Key={'player_id': player_id, 'leaderboard_id': lb_id}
        ).get('Item')

        current = int(existing.get('score', 0)) if existing else 0
        games = int(existing.get('games_played', 0)) if existing else 0

        # High-score-wins: only update if new score is higher
        new_score = max(int(score), current)
        inverted = make_inverted(new_score, player_id)

        table.put_item(Item={
            'player_id': player_id,
            'leaderboard_id': lb_id,
            'score': new_score,
            'inverted_score': inverted,
            'display_name': display_name,
            'avatar_url': f'https://api.dicebear.com/7.x/pixel-art/svg?seed={player_id}',
            'games_played': games + 1,
            'country': country,
            'last_score_at': now_iso,
        })

        results.append({
            'leaderboard': lb_id,
            'previous_score': current,
            'new_score': new_score,
            'improved': new_score > current,
        })

    return results


def lambda_handler(event, context):
    # Handle CORS preflight
    if event.get('httpMethod') == 'OPTIONS':
        return {'statusCode': 200, 'headers': CORS, 'body': ''}

    body = json.loads(event.get('body', '{}'))
    pid = body.get('player_id', '').strip().lower()
    score = body.get('score', 0)
    name = body.get('display_name', pid)
    country = body.get('country', 'US')

    if not pid:
        return {
            'statusCode': 400,
            'headers': CORS,
            'body': json.dumps({'error': 'player_id is required'}),
        }

    results = submit_score(pid, score, name, country)
    return {
        'statusCode': 200,
        'headers': CORS,
        'body': json.dumps({
            'player_id': pid,
            'updates': results,
        }, default=str),
    }

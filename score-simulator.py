import json
import random
import boto3
from datetime import datetime, timezone

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('leaderboard-scores')

MAX_SCORE = 999999
SCORE_PAD = 7

NAMES = [
    'ShadowNinja', 'BlazeMaster', 'CosmicWolf', 'ThunderStrike',
    'PixelQueen', 'NeonViper', 'FrostByte', 'TurboTank',
    'StarFury', 'IronPulse', 'CyberHawk', 'GhostRider',
    'VortexKing', 'PhoenixRise', 'ZeroGravity', 'MysticRaven',
    'SilverBolt', 'DarkMatter', 'LunarEcho', 'SolarFlare',
    'OmegaWave', 'AlphaStorm', 'BetaShield', 'DeltaForce',
    'GammaRay', 'EpsilonEdge', 'ZetaPrime', 'ThetaBurst',
    'IotaSpark', 'KappaKnight',
]
COUNTRIES = [
    'US', 'US', 'US', 'IN', 'IN', 'UK', 'DE', 'BR',
    'JP', 'KR', 'CA', 'AU', 'FR', 'MX', 'SE',
]

CORS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'POST,OPTIONS',
    'Content-Type': 'application/json',
}


def make_inverted(score, player_id):
    inv = MAX_SCORE - int(score)
    return f'{str(inv).zfill(SCORE_PAD)}#{player_id}'


def get_leaderboard_ids():
    now = datetime.now(timezone.utc)
    return [
        'all-time',
        f'daily-{now.strftime("%Y-%m-%d")}',
        f'weekly-{now.strftime("%Y-W%W")}',
    ]


def lambda_handler(event, context):
    if event.get('httpMethod') == 'OPTIONS':
        return {'statusCode': 200, 'headers': CORS, 'body': ''}

    body = json.loads(event.get('body', '{}'))
    num = min(body.get('players', 15), 30)
    now_iso = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    lb_ids = get_leaderboard_ids()

    results = []
    for i in range(num):
        name = NAMES[i % len(NAMES)]
        pid = name.lower()
        score = random.randint(500, 9999)
        country = random.choice(COUNTRIES)

        try:
            for lb_id in lb_ids:
                # Read existing score
                existing = table.get_item(
                    Key={'player_id': pid, 'leaderboard_id': lb_id}
                ).get('Item')

                current = int(existing.get('score', 0)) if existing else 0
                games = int(existing.get('games_played', 0)) if existing else 0

                # High-score wins
                new_score = max(score, current)
                inverted = make_inverted(new_score, pid)

                table.put_item(Item={
                    'player_id': pid,
                    'leaderboard_id': lb_id,
                    'score': new_score,
                    'inverted_score': inverted,
                    'display_name': name,
                    'avatar_url': f'https://api.dicebear.com/7.x/pixel-art/svg?seed={pid}',
                    'games_played': games + 1,
                    'country': country,
                    'last_score_at': now_iso,
                })

            results.append({'player': name, 'score': score})
        except Exception as e:
            results.append({'player': name, 'error': str(e)[:100]})

    return {
        'statusCode': 200,
        'headers': CORS,
        'body': json.dumps({
            'players_generated': len(results),
            'scores': sorted(results, key=lambda r: r.get('score', 0), reverse=True),
        }, default=str),
    }

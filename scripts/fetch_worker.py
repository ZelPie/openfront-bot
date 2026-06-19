import asyncio

async def fetch_game_worker(worker_id, session, queue, cancel_event, downloaded_games):
    while True:
        try:
            game = await queue.get()
        except asyncio.CancelledError:
            break
            
        gid = game.get("gameId")
        retries = 0
        
        while retries < 5:
            if cancel_event.is_set():
                break
            try:
                async with session.get(f"https://api.openfront.io/public/game/{gid}?turns=false", timeout=15) as g_resp:
                    if g_resp.status == 200:
                        g_data = await g_resp.json()
                        info = g_data.get("info", {})
                        
                        if not g_data or not info or not info.get("players"):
                            retries += 1
                            await asyncio.sleep(1)
                            continue
                            
                        downloaded_games[gid] = g_data
                        break
                    elif g_resp.status == 429:
                        await asyncio.sleep(2)
                    else:
                        downloaded_games[gid] = None
                        break
            except Exception:
                retries += 1
                await asyncio.sleep(1)
        
        if gid not in downloaded_games:
            downloaded_games[gid] = None
            
        queue.task_done()
        await asyncio.sleep(0.2)
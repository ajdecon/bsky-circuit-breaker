#!/usr/bin/env python3
"""
bsky-circuit-breaker

A lightweight cron script to monitor for your BlueSky posts going off the rails

Checks for:
    - Too many replies or quotes happening too quickly
    - Getting "ratio'd" with more replies than likes
    - Getting quote-posted by very large accounts

Optionally, this script can also use a local LLM (with ollama) to check for
"bad replies" as measured by sentiment analysis:
    - Hostile replies: personal insults, passive-aggressive replies, etc
    - Toxic behavior: threats, abuse, profanity
    - Slurs: hateful and dehumanizing language
    - Off-topic spam or thread hijacks

LLM analysis is disabled by default as this is a lot less reliable of a signal,
but still sometimes useful!
"""

import argparse
import json
import logging
import sqlite3
import sys
import tomllib
import urllib.request
from datetime import datetime, timedelta, timezone

from atproto import Client, models, exceptions

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger("circuit_breaker")

def load_config(config_path="config.toml"):
    try:
        with open(config_path, "rb") as f:
            return tomllib.load(f)
    except FileNotFoundError:
        logger.error(f"Configuration file {config_path} not found.")
        sys.exit(1)
    except tomllib.TOMLDecodeError as e:
        logger.error(f"Error parsing {config_path}: {e}")
        sys.exit(1)

def init_db(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON;")
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS posts (
        post_uri TEXT PRIMARY KEY,
        created_at TIMESTAMP NOT NULL,
        last_reply_count INTEGER DEFAULT 0,
        last_quote_count INTEGER DEFAULT 0,
        is_locked BOOLEAN DEFAULT FALSE,
        lock_reason TEXT DEFAULT NULL,
        handle TEXT DEFAULT NULL
    );
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS interactions (
        id TEXT PRIMARY KEY,
        post_uri TEXT NOT NULL,
        actor_did TEXT NOT NULL,
        actor_handle TEXT,
        raw_text TEXT NOT NULL,
        bluesky_created_at TIMESTAMP NOT NULL,
        llm_raw_analysis JSON,
        triggered_sentiments JSON DEFAULT '[]',
        is_counted_as_bad BOOLEAN DEFAULT FALSE,
        evaluated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (post_uri) REFERENCES posts(post_uri) ON DELETE CASCADE
    );
    """)
    conn.commit()
    return conn
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS interactions (
        id TEXT PRIMARY KEY,
        post_uri TEXT NOT NULL,
        actor_did TEXT NOT NULL,
        actor_handle TEXT,
        raw_text TEXT NOT NULL,
        bluesky_created_at TIMESTAMP NOT NULL,
        llm_raw_analysis JSON,
        triggered_sentiments JSON DEFAULT '[]',
        is_counted_as_bad BOOLEAN DEFAULT FALSE,
        evaluated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (post_uri) REFERENCES posts(post_uri) ON DELETE CASCADE
    );
    """)
    conn.commit()
    return conn

def get_iso_datetime(iso_str):
    return datetime.fromisoformat(iso_str.replace('Z', '+00:00'))

def extract_interactions(thread_node, root_uri):
    """Recursively traverses a thread view to extract all replies and quotes."""
    interactions = []
    
    # Current node is a post view
    if hasattr(thread_node, 'post'):
        post_view = thread_node.post
        if post_view.uri != root_uri:
            interactions.append(post_view)
            
    # Check for replies
    if hasattr(thread_node, 'replies') and thread_node.replies:
        for reply in thread_node.replies:
            interactions.extend(extract_interactions(reply, root_uri))
            
    # Check for quotes (if API embeds them directly in thread views in the future)
    if hasattr(thread_node, 'quotes') and thread_node.quotes:
        for quote in thread_node.quotes:
            interactions.extend(extract_interactions(quote, root_uri))
            
    return interactions

def apply_gates(client_write, did, post_uri, config):
    """Creates or updates Threadgates and Postgates on the user's repository."""
    rkey = post_uri.split('/')[-1]
    action_config = config['action_settings']
    
    # 1. Apply Threadgate
    if action_config.get('apply_threadgate', True):
        swap_cid = None
        try:
            existing = client_write.com.atproto.repo.get_record(
                models.ComAtprotoRepoGetRecord.Params(
                    repo=did,
                    collection='app.bsky.feed.threadgate',
                    rkey=rkey
                )
            )
            swap_cid = existing.value.cid if hasattr(existing, 'value') else existing.cid
        except exceptions.AtProtocolError:
            pass # No existing gate

        allow_rules = [{"$type": f"app.bsky.feed.threadgate#{rule}"} for rule in action_config.get('allowed_reply_rules', [])]
        
        record = {
            "$type": "app.bsky.feed.threadgate",
            "post": post_uri,
            "createdAt": datetime.now(timezone.utc).isoformat(),
            "allow": allow_rules
        }
        
        try:
            client_write.com.atproto.repo.put_record(
                models.ComAtprotoRepoPutRecord.Data(
                    repo=did,
                    collection='app.bsky.feed.threadgate',
                    rkey=rkey,
                    record=record,
                    swap_record=swap_cid
                )
            )
            logger.info(f"Applied Threadgate to {post_uri}")
        except Exception as e:
            logger.error(f"Failed to apply Threadgate to {post_uri}: {e}")

    # 2. Apply Postgate
    if action_config.get('apply_postgate', True):
        swap_cid = None
        try:
            existing = client_write.com.atproto.repo.get_record(
                models.ComAtprotoRepoGetRecord.Params(
                    repo=did,
                    collection='app.bsky.feed.postgate',
                    rkey=rkey
                )
            )
            swap_cid = existing.value.cid if hasattr(existing, 'value') else existing.cid
        except exceptions.AtProtocolError:
            pass 
            
        embedding_rules = []
        if action_config.get('disable_new_quotes', True):
            embedding_rules.append({"$type": "app.bsky.feed.postgate#disableRule"})
            
        record = {
            "$type": "app.bsky.feed.postgate",
            "post": post_uri,
            "createdAt": datetime.now(timezone.utc).isoformat(),
            "embeddingRules": embedding_rules
        }
        
        try:
            client_write.com.atproto.repo.put_record(
                models.ComAtprotoRepoPutRecord.Data(
                    repo=did,
                    collection='app.bsky.feed.postgate',
                    rkey=rkey,
                    record=record,
                    swap_record=swap_cid
                )
            )
            logger.info(f"Applied Postgate to {post_uri}")
        except Exception as e:
            logger.error(f"Failed to apply Postgate to {post_uri}: {e}")

def check_algorithmic_rules(post_uri, new_interactions, current_replies, current_quotes, likes, config, client_read, db):
    """
    Evaluates new interactions against deterministic algorithmic rules.
    Returns the rule tripped (str) or None.
    """
    algo = config['algorithmic_rules']
    
    # 1. Total Volume
    total_interactions = current_replies + current_quotes
    logger.debug(f"Checking volume rule for {post_uri}: total={total_interactions}, max={algo['max_total_interactions']}")
    if total_interactions >= algo['max_total_interactions']:
        logger.info(f"Volume rule triggered for {post_uri}: exceeded max total interactions")
        return "ALGORITHMIC_VOLUME_EXCEEDED"
        
    # 2. Ratio Check
    safe_likes = max(1, likes)
    ratio = current_replies / safe_likes
    logger.debug(f"Checking ratio rule for {post_uri}: replies={current_replies}, likes={likes}, ratio={ratio}")
    if current_replies >= algo['minimum_replies_for_ratio'] and ratio >= algo['reply_to_like_ratio_threshold']:
        logger.info(f"Ratio rule triggered for {post_uri}: ratio {ratio} exceeds threshold {algo['reply_to_like_ratio_threshold']}")
        return "ALGORITHMIC_RATIO_EXCEEDED"
        
    # 3. Rate Velocity (Rolling 1 Hour)
    cursor = db.cursor()
    cursor.execute("SELECT count(*) FROM interactions WHERE post_uri = ? AND bluesky_created_at >= ?", 
                   (post_uri, (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()))
    recent_hour_count = cursor.fetchone()[0]
    logger.debug(f"Checking rate rule for {post_uri}: recent_count={recent_hour_count}, max={algo['max_interactions_per_hour']}")
    if recent_hour_count >= algo['max_interactions_per_hour']:
        logger.info(f"Rate rule triggered for {post_uri}: exceeded max interactions per hour")
        return "ALGORITHMIC_RATE_EXCEEDED"
        
    # 4. Follower Amplification (Batch Profile Fetch)
    new_actor_dids = list(set([inter.author.did for inter in new_interactions]))
    if new_actor_dids:
        try:
            # Split into chunks of 25 (atproto limit for getProfiles)
            for i in range(0, len(new_actor_dids), 25):
                chunk = new_actor_dids[i:i+25]
                logger.debug(f"Fetching profiles for {len(chunk)} actors")
                prof_resp = client_read.app.bsky.actor.get_profiles({'actors': chunk})
                for p in prof_resp.profiles:
                    # Handle different possible attribute names based on atproto library version
                    followers_count = getattr(p, 'followersCount', None) or getattr(p, 'followers_count', 0)
                    if followers_count >= algo['follower_amplification_threshold']:
                        logger.info(f"Amplification rule triggered for {post_uri}: actor {p.handle} has {followers_count} followers")
                        return f"ALGORITHMIC_AMPLIFICATION_BY_{p.handle}"
        except Exception as e:
            logger.error(f"Profile batch fetch failed: {e}")

    logger.debug(f"No algorithmic rules triggered for {post_uri}")
    return None

def evaluate_llm(text, config):
    """Sends text to local Ollama API and evaluates sentiment."""
    llm_conf = config['llm']
    rules = config['sentiment_rules']['criteria']
    
    prompt = f"Analyze the text and return valid JSON with scores (0.0 to 1.0) for the criteria.\n\nText: {text}\n\nCriteria:\n"
    active_keys = []
    for key, c in rules.items():
        if c.get('enabled', False):
            prompt += f"- {key}: {c['definition']}\n"
            active_keys.append(key)
            
    prompt += "\nOutput format: {\"scores\": {\"hostile\": 0.0, ...}, \"reasoning\": \"...\"}"
    
    req_body = json.dumps({
        "model": llm_conf['model_name'],
        "prompt": prompt,
        "format": "json",
        "stream": False
    }).encode('utf-8')
    
    req = urllib.request.Request(f"{llm_conf['api_base']}/api/generate", data=req_body, headers={'Content-Type': 'application/json'})
    
    try:
        with urllib.request.urlopen(req, timeout=llm_conf.get('timeout_seconds', 60)) as response:
            res_data = json.loads(response.read().decode())
            analysis = json.loads(res_data['response'])
            
            triggered = []
            scores = analysis.get('scores', {})
            for key in active_keys:
                if scores.get(key, 0.0) >= rules[key]['threshold']:
                    triggered.append(key)
                    
            logger.debug(f"LLM evaluation completed for text: {text[:50]}... - triggered: {triggered}")
            return analysis, triggered
    except Exception as e:
        logger.error(f"LLM Error: {e}")
        return None, []

def run_llm_evaluations(db, config, handle, app_password, user_did, dry_run):
    """
    Evaluates pending interactions using the local LLM, and triggers locks if limits are breached.
    """
    if config['run_settings'].get('disable_llm', False):
        return

    cursor = db.cursor()
    cursor.execute("""
        SELECT i.id, i.raw_text, i.post_uri 
        FROM interactions i
        JOIN posts p ON i.post_uri = p.post_uri
        WHERE i.llm_raw_analysis IS NULL AND p.is_locked = 0
        LIMIT ?
    """, (config['llm'].get('max_llm_evaluations_per_run', 15),))
    
    unscored = cursor.fetchall()
    
    # Instantiate the write client only if necessary
    client_write = None
    
    for inter in unscored:
        analysis, triggered = evaluate_llm(inter['raw_text'], config)
        is_bad = 1 if triggered else 0
        
        cursor.execute("UPDATE interactions SET llm_raw_analysis = ?, triggered_sentiments = ?, is_counted_as_bad = ? WHERE id = ?",
                       (json.dumps(analysis), json.dumps(triggered), is_bad, inter['id']))
        db.commit()
        
        # Check bad interaction threshold
        cursor.execute("SELECT count(*) FROM interactions WHERE post_uri = ? AND is_counted_as_bad = 1", (inter['post_uri'],))
        bad_count = cursor.fetchone()[0]
        
        if bad_count >= config['sentiment_rules']['max_bad_interactions']:
            logger.info(f"Post {inter['post_uri']} tripped LLM rule: MAX_BAD_INTERACTIONS. Locking...")
            if not dry_run:
                if not client_write:
                    client_write = Client()
                    client_write.login(handle, app_password)
                apply_gates(client_write, user_did, inter['post_uri'], config)
                
            cursor.execute("UPDATE posts SET is_locked = 1, lock_reason = ? WHERE post_uri = ?", ("LLM_HOSTILITY_THRESHOLD", inter['post_uri']))
            db.commit()

def run_report(db, current_handle=None):
    """Generates a summary report of recent circuit breaker activity."""
    cursor = db.cursor()
    print("\n" + "="*50)
    print("BSKY-CIRCUIT-BREAKER REPORT")
    print("="*50)
    
    # Filter by current handle if specified
    if current_handle:
        cursor.execute("SELECT COUNT(*) FROM posts WHERE is_locked = 1 AND handle = ?", (current_handle,))
    else:
        cursor.execute("SELECT COUNT(*) FROM posts WHERE is_locked = 1")
    locked_count = cursor.fetchone()[0]
    print(f"Total Locked Posts: {locked_count}")
    
    if current_handle:
        cursor.execute("SELECT post_uri, lock_reason, created_at FROM posts WHERE is_locked = 1 AND handle = ? ORDER BY created_at DESC LIMIT 10", (current_handle,))
    else:
        cursor.execute("SELECT post_uri, lock_reason, created_at FROM posts WHERE is_locked = 1 ORDER BY created_at DESC LIMIT 10")
    recent_locks = cursor.fetchall()
    if recent_locks:
        print("\nRecent Locked Posts:")
        for r in recent_locks:
            # Convert at:// URI to https://bsky.app URL
            post_url = r['post_uri'].replace('at://', 'https://bsky.app/profile/')
            post_url = post_url.replace('/app.bsky.feed.post/', '/post/')
            print(f" - {post_url} | Reason: {r['lock_reason']} | Locked On: {r['created_at']}")
            
    if current_handle:
        cursor.execute("SELECT COUNT(*) FROM interactions WHERE is_counted_as_bad = 1 AND post_uri IN (SELECT post_uri FROM posts WHERE handle = ?)", (current_handle,))
    else:
        cursor.execute("SELECT COUNT(*) FROM interactions WHERE is_counted_as_bad = 1")
    bad_count = cursor.fetchone()[0]
    print(f"\nTotal LLM-Flagged Interactions: {bad_count}")
    
    # Show details of flagged interactions if any exist
    if bad_count > 0:
        print("\nFlagged Interaction Details:")
        if current_handle:
            cursor.execute("""
                SELECT i.id, i.post_uri, i.actor_handle, i.raw_text, i.triggered_sentiments, i.llm_raw_analysis 
                FROM interactions i 
                JOIN posts p ON i.post_uri = p.post_uri
                WHERE i.is_counted_as_bad = 1 AND p.handle = ?
                ORDER BY i.evaluated_at DESC 
                LIMIT 20
            """, (current_handle,))
        else:
            cursor.execute("""
                SELECT i.id, i.post_uri, i.actor_handle, i.raw_text, i.triggered_sentiments, i.llm_raw_analysis 
                FROM interactions i 
                WHERE i.is_counted_as_bad = 1 
                ORDER BY i.evaluated_at DESC 
                LIMIT 20
            """)
        flagged = cursor.fetchall()
        for f in flagged:
            # Convert at:// URI to https://bsky.app URL
            post_url = f['post_uri'].replace('at://', 'https://bsky.app/profile/')
            post_url = post_url.replace('/app.bsky.feed.post/', '/post/')
            print(f" - Post: {post_url}")
            print(f"   Actor: {f['actor_handle']}")
            print(f"   Text: {f['raw_text'][:100]}{'...' if len(f['raw_text']) > 100 else ''}")
            if f['triggered_sentiments']:
                print(f"   Triggered criteria: {', '.join(json.loads(f['triggered_sentiments']))}")
            if f['llm_raw_analysis']:
                analysis = json.loads(f['llm_raw_analysis'])
                if 'reasoning' in analysis:
                    print(f"   LLM Explanation: {analysis['reasoning']}")
            print()
    
    print("="*50 + "\n")

def main():
    parser = argparse.ArgumentParser(description="bsky-circuit-breaker: automated moderation script.")
    parser.add_argument("--run", action="store_true", help="Execute the main evaluation loop.")
    parser.add_argument("--report", action="store_true", help="Generate a summary report.")
    parser.add_argument("--handle", type=str, help="Override configured user handle.")
    parser.add_argument("--app-password", type=str, help="Override configured app password.")
    
    args = parser.parse_args()
    config = load_config()
    
    handle = args.handle or config['credentials']['handle']
    app_password = args.app_password or config['credentials'].get('app_password')
    
    # Dry-run enforcement logic
    dry_run = False
    if args.handle and not args.app_password:
        logger.warning("Running in DRY-RUN mode because handle was overridden without providing an app password.")
        dry_run = True
        args.report = True
        
    db = init_db(config['run_settings']['db_path'])
    
    if args.run:
        logger.info(f"Starting execution cycle for {handle}...")
        
        # Unauthenticated Read Client
        client_read = Client(base_url="https://public.api.bsky.app")
        
        logger.debug(f"Fetching author feed for {handle}...")
        try:
            feed_resp = client_read.app.bsky.feed.get_author_feed({'actor': handle})
            profile = client_read.app.bsky.actor.get_profile({'actor': handle})
            user_did = profile.did
        except Exception as e:
            logger.error(f"Failed to fetch initial state for {handle}: {e}")
            sys.exit(1)
            
        lookback_limit = datetime.now(timezone.utc) - timedelta(days=config['run_settings']['lookback_days'])
        
        for feed_view in feed_resp.feed:
            post = feed_view.post
            post_date = get_iso_datetime(post.record.created_at)
            
            # Skip reposts (posts with no text and no media, as they are likely reposts)
            if not getattr(post.record, 'text', None) and not getattr(post.record, 'embed', None):
                continue
                
            if post_date < lookback_limit:
                continue
                
            cursor = db.cursor()
            cursor.execute("SELECT last_reply_count, last_quote_count, is_locked FROM posts WHERE post_uri = ?", (post.uri,))
            row = cursor.fetchone()
            
            if row and row['is_locked']:
                continue
                
            current_replies = post.reply_count or 0
            current_quotes = post.quote_count or 0
            
            if not row:
                cursor.execute("INSERT INTO posts (post_uri, created_at, last_reply_count, last_quote_count, handle) VALUES (?, ?, ?, ?, ?)",
                               (post.uri, post.record.created_at, current_replies, current_quotes, handle))
                db.commit()
                row_replies, row_quotes = 0, 0
            else:
                row_replies, row_quotes = row['last_reply_count'], row['last_quote_count']
                
            # Sync & Algorithmic Evaluator
            if current_replies > row_replies or current_quotes > row_quotes:
                logger.info(f"New interactions detected for {post.uri}. Fetching thread...")
                thread_resp = client_read.app.bsky.feed.get_post_thread({'uri': post.uri})
                new_interactions = extract_interactions(thread_resp.thread, post.uri)
                
                # Sync new interactions to database
                for inter in new_interactions:
                    cursor.execute("INSERT OR IGNORE INTO interactions (id, post_uri, actor_did, actor_handle, raw_text, bluesky_created_at) VALUES (?, ?, ?, ?, ?, ?)",
                                   (inter.uri, post.uri, inter.author.did, inter.author.handle, getattr(inter.record, 'text', ''), inter.record.created_at))
                
                # Update interaction sync counts
                cursor.execute("UPDATE posts SET last_reply_count = ?, last_quote_count = ? WHERE post_uri = ?", (current_replies, current_quotes, post.uri))
                db.commit()

                # Run Algorithmic Rules
                likes = post.like_count or 0
                lock_reason = check_algorithmic_rules(post.uri, new_interactions, current_replies, current_quotes, likes, config, client_read, db)
                
                # Apply lock if tripped algorithmically
                if lock_reason:
                    logger.warning(f"Post {post.uri} tripped rule: {lock_reason}. Locking...")
                    if not dry_run:
                        client_write = Client()
                        client_write.login(handle, app_password)
                        apply_gates(client_write, user_did, post.uri, config)
                        
                    cursor.execute("UPDATE posts SET is_locked = 1, lock_reason = ? WHERE post_uri = ?", (lock_reason, post.uri))
                    db.commit()
                    
        # LLM Evaluation & Sentiment
        if not config['run_settings'].get('disable_llm', False):
            run_llm_evaluations(db, config, handle, app_password, user_did, dry_run)
        else:
            logger.info("LLM evaluations disabled by configuration.")

    # Reporting Engine
    if args.report:
        run_report(db, handle)

if __name__ == "__main__":
    main()


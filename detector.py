"""
Solana Narrative Detector
Detects emerging meme coin narratives from on-chain clustering + X scraping.
Runs every 30 min via GitHub Actions. Outputs data/narratives.json for dashboard.
"""

import os
import json
import time
import math
import hashlib
import requests
import re
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# ── Config ──────────────────────────────────────────────────────────────────
HELIUS_KEY   = os.environ["HELIUS_KEY"]
HELIUS_RPC   = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_KEY}"
HELIUS_API   = f"https://api.helius.xyz/v0"

DATA_FILE    = "data/narratives.json"
LOG_FILE     = "data/token_log.csv"
LAST_RUN     = "data/last_run.txt"

# How far back to look for new tokens (minutes)
LOOKBACK_MIN = 60

# Minimum tokens sharing a theme to call it a narrative
MIN_CLUSTER  = 2

# DexScreener endpoint (free, no key)
DEX_NEW_URL  = "https://api.dexscreener.com/latest/dex/search?q=solana"
DEX_PAIRS_URL = "https://api.dexscreener.com/latest/dex/tokens/"

# X scrape targets — free nitter instances (no API key)
NITTER_HOSTS = [
    "https://nitter.poast.org",
    "https://nitter.privacydev.net",
    "https://nitter.net",
]

# Known narrative seed keywords — system will also discover novel ones
SEED_NARRATIVES = {
    "ai":        ["ai", "artificial intelligence", "agent", "gpt", "llm", "robot", "neural", "agi", "skynet"],
    "animal":    ["dog", "cat", "frog", "pepe", "doge", "shib", "wolf", "bear", "bull", "ape", "monkey", "fish", "bird"],
    "political": ["trump", "maga", "biden", "kamala", "elon", "president", "vote", "election", "congress"],
    "gaming":    ["game", "gaming", "nft", "metaverse", "play", "arcade", "pixel", "rpg"],
    "defi":      ["defi", "yield", "stake", "lend", "protocol", "dao", "governance", "vault"],
    "desci":     ["science", "desci", "research", "bio", "dna", "quantum", "physics", "chem"],
    "celebrity": ["celebrity", "viral", "tiktok", "youtube", "streamer", "twitch", "influencer"],
    "meme":      ["meme", "wojak", "chad", "based", "cope", "seethe", "wagmi", "ngmi", "gm", "lfg"],
}

# ── Helius helpers ───────────────────────────────────────────────────────────

def helius_rpc(method, params):
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    try:
        r = requests.post(HELIUS_RPC, json=payload, timeout=15)
        r.raise_for_status()
        return r.json().get("result")
    except Exception as e:
        print(f"  [RPC error] {method}: {e}")
        return None


def get_new_pump_tokens(lookback_minutes=LOOKBACK_MIN):
    """
    Fetch recently launched tokens using DexScreener's new-pairs endpoint
    (free, no key) filtered to Solana + Pump.fun. Falls back to Helius
    token-metadata search if DexScreener returns nothing.
    """
    print(f"[1] Fetching new Solana tokens (last {lookback_minutes}min)...")
    cutoff_ts = int((datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)).timestamp())

    tokens = []

    # ── Strategy 1: DexScreener latest Solana pairs (free, reliable) ──
    try:
        r = requests.get(
            "https://api.dexscreener.com/token-profiles/latest/v1",
            timeout=15
        )
        if r.status_code == 200:
            profiles = r.json() if isinstance(r.json(), list) else []
            for p in profiles:
                if p.get("chainId") != "solana":
                    continue
                addr = p.get("tokenAddress", "")
                if addr and len(addr) > 30:
                    tokens.append({
                        "mint":   addr,
                        "name":   p.get("header", "") or "",
                        "symbol": "",
                        "description": p.get("description", "") or "",
                        "ts":     int(datetime.now(timezone.utc).timestamp()),
                    })
            print(f"  DexScreener profiles: {len(tokens)} Solana tokens")
    except Exception as e:
        print(f"  [DexScreener profile error] {e}")

    # ── Strategy 2: DexScreener boosted tokens (catches trending new ones) ──
    try:
        r2 = requests.get(
            "https://api.dexscreener.com/token-boosts/latest/v1",
            timeout=15
        )
        if r2.status_code == 200:
            boosts = r2.json() if isinstance(r2.json(), list) else []
            existing_mints = {t["mint"] for t in tokens}
            for b in boosts:
                if b.get("chainId") != "solana":
                    continue
                addr = b.get("tokenAddress", "")
                if addr and addr not in existing_mints:
                    tokens.append({
                        "mint":   addr,
                        "name":   b.get("description", "") or "",
                        "symbol": "",
                        "description": b.get("description", "") or "",
                        "ts":     int(datetime.now(timezone.utc).timestamp()),
                    })
                    existing_mints.add(addr)
            print(f"  DexScreener boosts added. Total: {len(tokens)}")
    except Exception as e:
        print(f"  [DexScreener boost error] {e}")

    # ── Strategy 3: DexScreener search for "pump" new pairs ──
    if len(tokens) < 10:
        try:
            r3 = requests.get(
                "https://api.dexscreener.com/latest/dex/search?q=pump",
                timeout=15
            )
            if r3.status_code == 200:
                pairs = r3.json().get("pairs", []) or []
                existing_mints = {t["mint"] for t in tokens}
                for p in pairs:
                    if p.get("chainId") != "solana":
                        continue
                    # Only include pairs created recently
                    created = p.get("pairCreatedAt", 0) or 0
                    if isinstance(created, int) and created > 0:
                        created_ts = created // 1000  # ms → s
                        if created_ts < cutoff_ts:
                            continue
                    base = p.get("baseToken", {})
                    addr = base.get("address", "")
                    if addr and addr not in existing_mints and len(addr) > 30:
                        tokens.append({
                            "mint":   addr,
                            "name":   base.get("name", "") or "",
                            "symbol": base.get("symbol", "") or "",
                            "description": "",
                            "ts":     created // 1000 if created else int(datetime.now(timezone.utc).timestamp()),
                            "liq_usd":  float(p.get("liquidity", {}).get("usd", 0) or 0),
                            "vol_24h":  float(p.get("volume", {}).get("h24", 0) or 0),
                            "buys_1h":  int(p.get("txns", {}).get("h1", {}).get("buys", 0) or 0),
                            "sells_1h": int(p.get("txns", {}).get("h1", {}).get("sells", 0) or 0),
                            "mcap":     float(p.get("fdv", 0) or 0),
                            "dex_url":  p.get("url", ""),
                        })
                        existing_mints.add(addr)
                print(f"  DexScreener search added. Total: {len(tokens)}")
        except Exception as e:
            print(f"  [DexScreener search error] {e}")

    print(f"  {len(tokens)} unique tokens found")
    return tokens


def enrich_token(mint, existing=None):
    """
    Get token metadata. Uses data already fetched from DexScreener if available,
    otherwise queries DexScreener pairs endpoint. Avoids Helius RPC for individual txns.
    """
    # If we already have good data from the fetch step, use it
    if existing and (existing.get("name") or existing.get("symbol")):
        # Still try to get DEX data if missing
        if not existing.get("dex_url"):
            dex = get_dex_data(mint)
            return {**existing, **dex}
        return existing

    # Otherwise fetch from DexScreener
    dex = get_dex_data(mint)
    return {"name": "", "symbol": "", "description": "", **dex}


def enrich_token_meta(mint):
    """Get token metadata from Helius DAS API (batch-safe, used sparingly)."""
    try:
        r = requests.post(
            f"{HELIUS_API}/token-metadata?api-key={HELIUS_KEY}",
            json={"mintAccounts": [mint], "includeOffChain": True, "disableCache": False},
            timeout=10
        )
        r.raise_for_status()
        data = r.json()
        if data and len(data) > 0:
            item = data[0]
            on_chain = item.get("onChainMetadata", {}).get("metadata", {}).get("data", {})
            off_chain = item.get("offChainMetadata", {}).get("metadata", {}) or {}
            name = on_chain.get("name", "") or off_chain.get("name", "") or ""
            symbol = on_chain.get("symbol", "") or off_chain.get("symbol", "") or ""
            desc = off_chain.get("description", "") or ""
            return {"name": name, "symbol": symbol, "description": desc}
    except Exception as e:
        print(f"  [enrich error] {mint[:8]}: {e}")
    return {"name": "", "symbol": "", "description": ""}


def get_dex_data(mint):
    """Get price/liquidity/volume from DexScreener."""
    try:
        r = requests.get(DEX_PAIRS_URL + mint, timeout=10)
        r.raise_for_status()
        pairs = r.json().get("pairs", [])
        sol_pairs = [p for p in pairs if p.get("chainId") == "solana"]
        if not sol_pairs:
            return {}
        p = max(sol_pairs, key=lambda x: float(x.get("liquidity", {}).get("usd", 0) or 0))
        return {
            "price_usd":  float(p.get("priceUsd", 0) or 0),
            "liq_usd":    float(p.get("liquidity", {}).get("usd", 0) or 0),
            "vol_24h":    float(p.get("volume", {}).get("h24", 0) or 0),
            "buys_1h":    int(p.get("txns", {}).get("h1", {}).get("buys", 0) or 0),
            "sells_1h":   int(p.get("txns", {}).get("h1", {}).get("sells", 0) or 0),
            "mcap":       float(p.get("fdv", 0) or 0),
            "pair_addr":  p.get("pairAddress", ""),
            "dex_url":    p.get("url", ""),
        }
    except Exception:
        return {}


# ── X Scraping (no API key) ──────────────────────────────────────────────────

def scrape_x_keyword(keyword, max_posts=30):
    """Scrape X via nitter for a keyword. Returns list of {text, author, ts}."""
    for host in NITTER_HOSTS:
        try:
            url = f"{host}/search?q={requests.utils.quote(keyword + ' solana')}&f=tweets"
            r = requests.get(url, timeout=10, headers={
                "User-Agent": "Mozilla/5.0 (compatible; research-bot/1.0)"
            })
            if r.status_code != 200:
                continue

            posts = []
            # Parse nitter HTML for tweet text
            import html
            text_blocks = re.findall(
                r'<div class="tweet-content[^"]*"[^>]*>(.*?)</div>',
                r.text, re.DOTALL
            )
            author_blocks = re.findall(
                r'<a class="username"[^>]*>@([^<]+)</a>',
                r.text
            )
            time_blocks = re.findall(
                r'<span class="tweet-date"[^>]*><a[^>]*title="([^"]+)"',
                r.text
            )

            for i, block in enumerate(text_blocks[:max_posts]):
                clean = re.sub(r'<[^>]+>', '', block)
                clean = html.unescape(clean).strip()
                if len(clean) < 10:
                    continue
                posts.append({
                    "text":   clean[:280],
                    "author": author_blocks[i] if i < len(author_blocks) else "unknown",
                    "ts":     time_blocks[i] if i < len(time_blocks) else "",
                    "source": "x"
                })

            if posts:
                print(f"  [X scrape] '{keyword}': {len(posts)} posts from {host}")
                return posts

        except Exception as e:
            print(f"  [X scrape error] {host}: {e}")
            continue
    return []


def scrape_social_signals(narrative_keywords):
    """Scrape X for each active narrative to get social velocity."""
    results = {}
    for narrative, keywords in narrative_keywords.items():
        # Pick the 2 most distinctive keywords to scrape
        targets = keywords[:2]
        posts = []
        for kw in targets:
            posts.extend(scrape_x_keyword(kw, max_posts=20))
            time.sleep(1)  # be polite

        # Unique authors
        authors = set(p["author"] for p in posts if p["author"] != "unknown")
        results[narrative] = {
            "post_count":    len(posts),
            "unique_authors": len(authors),
            "posts_sample":  [p["text"][:100] for p in posts[:3]],
        }
    return results


# ── Narrative Clustering ─────────────────────────────────────────────────────

def tokenize(text):
    """Simple word tokenizer, lowercased."""
    return set(re.findall(r'\b[a-z]{2,}\b', text.lower()))


def classify_token(name, symbol, description):
    """Classify a token into narrative categories. Returns list of matching narratives."""
    text = f"{name} {symbol} {description}".lower()
    words = tokenize(text)
    matches = []
    for narrative, keywords in SEED_NARRATIVES.items():
        if any(kw in words or kw in text for kw in keywords):
            matches.append(narrative)
    return matches if matches else ["novel"]


def detect_novel_clusters(tokens):
    """
    Find clusters of tokens that share unusual word patterns not in SEED_NARRATIVES.
    Simple approach: extract top bigrams/unigrams from token names, group by shared terms.
    """
    word_to_tokens = defaultdict(list)
    stop = {"the", "and", "for", "are", "but", "not", "you", "all", "can",
            "her", "was", "one", "our", "had", "his", "has", "him", "its",
            "sol", "solana", "token", "coin", "pump", "fun", "inu", "erc"}

    for t in tokens:
        words = tokenize(t.get("name", "") + " " + t.get("symbol", ""))
        for w in words:
            if w not in stop and len(w) >= 3:
                word_to_tokens[w].append(t.get("mint", ""))

    # Find words shared by 2+ tokens that aren't already in seed narratives
    known_words = set()
    for kws in SEED_NARRATIVES.values():
        known_words.update(kws)

    novel = {}
    for word, mints in word_to_tokens.items():
        if len(mints) >= MIN_CLUSTER and word not in known_words:
            novel[word] = mints

    return novel


# ── Scoring ──────────────────────────────────────────────────────────────────

def score_narrative(token_count, unique_buyers_total, liq_total,
                    social_posts, social_authors, crowding_penalty):
    """
    Score a narrative 0-100.
    Components:
      - token_count (breadth): how many distinct tokens
      - unique_buyers (adoption): wallet activity
      - liquidity (tradability): actual $ in pools
      - social velocity (attention): posts + unique authors
      - crowding penalty: too many tokens = late
    """
    # Token breadth: 2 tokens = 20pts, 5+ = 40pts (but penalize >10)
    breadth = min(40, token_count * 8) if token_count <= 5 else max(0, 40 - (token_count - 5) * 3)

    # Adoption: unique buyers across tokens
    adoption = min(25, math.log1p(unique_buyers_total) * 4)

    # Liquidity: $500 = 10pts, $10k = 20pts, $100k = 25pts
    liq_score = min(25, math.log1p(liq_total / 100) * 3) if liq_total > 0 else 0

    # Social: posts + 2x weight for unique authors
    social_raw = social_posts + (social_authors * 2)
    social_score = min(10, math.log1p(social_raw) * 1.5)

    raw = breadth + adoption + liq_score + social_score
    final = max(0, raw * (1 - crowding_penalty))
    return round(final, 1)


def crowding_penalty(token_count):
    """Returns 0.0–0.8 penalty for overcrowded narratives."""
    if token_count <= 3:   return 0.0
    if token_count <= 6:   return 0.1
    if token_count <= 10:  return 0.25
    if token_count <= 20:  return 0.45
    return 0.7


def lifecycle_stage(score, token_count, social_authors):
    """Estimate lifecycle stage."""
    if token_count < 2:                       return "👀 Watching"
    if score >= 60 and token_count <= 5:      return "🔥 Forming"
    if score >= 40 and token_count <= 10:     return "📈 Expanding"
    if token_count > 10 or score < 25:        return "🌊 Crowded"
    return "🟡 Early"


# ── Main ─────────────────────────────────────────────────────────────────────

def run():
    print(f"\n{'='*60}")
    print(f"Narrative Detector — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}")

    # 1. Get new tokens from Pump.fun
    raw_tokens = get_new_pump_tokens(LOOKBACK_MIN)

    # 2. Enrich with metadata + DEX data
    print(f"\n[2] Enriching {len(raw_tokens)} tokens...")
    enriched = []
    for t in raw_tokens:
        enriched_t = enrich_token(t.get("mint", ""), existing=t)
        # Skip tokens with no name AND no symbol AND no dex data
        if not enriched_t.get("name") and not enriched_t.get("symbol") and not enriched_t.get("dex_url"):
            continue
        enriched.append(enriched_t)
        time.sleep(0.05)
    print(f"  {len(enriched)} tokens with metadata")

    # 3. Classify into narratives
    print(f"\n[3] Classifying tokens into narratives...")
    narrative_tokens = defaultdict(list)
    for t in enriched:
        cats = classify_token(t.get("name",""), t.get("symbol",""), t.get("description",""))
        for cat in cats:
            narrative_tokens[cat].append(t)

    # 4. Detect novel clusters
    novel = detect_novel_clusters(enriched)
    for word, mints in novel.items():
        cluster_tokens = [t for t in enriched if t.get("mint") in mints]
        if len(cluster_tokens) >= MIN_CLUSTER:
            narrative_tokens[f"novel:{word}"].extend(cluster_tokens)

    # 5. Scrape X for active narratives
    print(f"\n[4] Scraping X for social signals...")
    active_narratives = {k: SEED_NARRATIVES.get(k, [k.replace("novel:", "")])
                        for k in narrative_tokens if len(narrative_tokens[k]) >= MIN_CLUSTER}
    social = scrape_social_signals(active_narratives)

    # 6. Score and build output
    print(f"\n[5] Scoring narratives...")
    narratives_out = []
    for name, tokens in narrative_tokens.items():
        if len(tokens) < MIN_CLUSTER:
            continue

        total_buyers = sum(t.get("buys_1h", 0) for t in tokens)
        total_liq    = sum(t.get("liq_usd", 0) for t in tokens)
        soc          = social.get(name, {})
        penalty      = crowding_penalty(len(tokens))
        score        = score_narrative(
            len(tokens), total_buyers, total_liq,
            soc.get("post_count", 0), soc.get("unique_authors", 0),
            penalty
        )
        stage = lifecycle_stage(score, len(tokens), soc.get("unique_authors", 0))

        # Top 3 tokens by liquidity
        top_tokens = sorted(tokens, key=lambda x: x.get("liq_usd", 0), reverse=True)[:5]

        narratives_out.append({
            "narrative":      name,
            "score":          score,
            "stage":          stage,
            "token_count":    len(tokens),
            "total_liq_usd":  round(total_liq, 2),
            "total_buys_1h":  total_buyers,
            "social_posts":   soc.get("post_count", 0),
            "social_authors": soc.get("unique_authors", 0),
            "social_samples": soc.get("posts_sample", []),
            "crowding":       round(penalty, 2),
            "top_tokens": [{
                "name":    t.get("name", "?"),
                "symbol":  t.get("symbol", "?"),
                "mint":    t.get("mint", ""),
                "liq_usd": round(t.get("liq_usd", 0), 2),
                "mcap":    round(t.get("mcap", 0), 2),
                "buys_1h": t.get("buys_1h", 0),
                "dex_url": t.get("dex_url", ""),
                "ts":      t.get("ts", 0),
            } for t in top_tokens],
            "updated_at": int(datetime.now(timezone.utc).timestamp()),
        })

    # Sort by score descending
    narratives_out.sort(key=lambda x: x["score"], reverse=True)

    # 7. Load previous run and merge history
    history = []
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE) as f:
                old = json.load(f)
                history = old.get("history", [])
        except Exception:
            pass

    # Keep last 48 snapshots in history (48 x 30min = 24h)
    history.append({
        "ts": int(datetime.now(timezone.utc).timestamp()),
        "narratives": [{
            "narrative": n["narrative"],
            "score":     n["score"],
            "stage":     n["stage"],
            "token_count": n["token_count"],
        } for n in narratives_out[:10]]
    })
    history = history[-48:]

    output = {
        "generated_at":    datetime.now(timezone.utc).isoformat(),
        "generated_ts":    int(datetime.now(timezone.utc).timestamp()),
        "tokens_scanned":  len(enriched),
        "narratives":      narratives_out,
        "history":         history,
    }

    os.makedirs("data", exist_ok=True)
    with open(DATA_FILE, "w") as f:
        json.dump(output, f, indent=2)

    with open(LAST_RUN, "w") as f:
        f.write(f"{datetime.now(timezone.utc).isoformat()}\n")
        f.write(f"Tokens scanned: {len(enriched)}\n")
        f.write(f"Narratives found: {len(narratives_out)}\n")
        for n in narratives_out[:5]:
            f.write(f"  {n['stage']} {n['narrative']}: score={n['score']} tokens={n['token_count']} liq=${n['total_liq_usd']:,.0f}\n")

    print(f"\n✅ Done. {len(narratives_out)} narratives found.")
    for n in narratives_out[:8]:
        print(f"  {n['stage']} [{n['score']:5.1f}] {n['narrative']:20s} | {n['token_count']} tokens | ${n['total_liq_usd']:>10,.0f} liq | {n['social_posts']} posts")


if __name__ == "__main__":
    run()

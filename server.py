import os
from flask import Flask, jsonify, make_response
from flask_cors import CORS
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import json
import csv
from datetime import datetime
import time
import hashlib


app = Flask(__name__)
CORS(app) # allow all origins by default; in production, restrict to YOUR Vercel domain

# Cache configuration
CACHE = {}
CACHE_TTL = 3600  # 1 hour in seconds
CACHE_MAX_SIZE = 100  # Maximum number of cached items

# 1) Configure your MongoDB URI (local or Atlas).
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI, server_api=ServerApi('1'))
client.admin.command('ping')
db = client.get_database(name="WPTable")

# 2) Collections
win_col = db["wins"]
prob_col = db['Probabilities']
delim_col = db["Delim"]
matches_col = db["matches"]  # Add matches collection

print(f"Win documents: {win_col.count_documents({})}")
print(f"Delim documents: {delim_col.count_documents({})}")
print(f"Matches documents: {matches_col.count_documents({})}")

# 3) We know exactly which ranks exist and in which order.
RANK_ORDER = [str(i) for i in range(1, 21)] + ["unranked"] # ["1","2",…,"20","unranked"]


with open("mens_waterpolo_rankings.json", "r", encoding="utf-8") as f:
    rankings = json.load(f)

# Load team name mappings
def load_team_mappings():
    """Load team name mappings from CSV file"""
    mappings = {}
    canonical_names = {}  # Map team_id to canonical name
    
    try:
        with open("WP_team_name_mappings.csv", "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                team_name = row['team_name'].strip()
                team_id = int(row['team_id'])
                
                # Map various team names to team_id
                mappings[team_name] = team_id
                
                # Keep track of canonical names (first occurrence for each team_id)
                if team_id not in canonical_names:
                    canonical_names[team_id] = team_name
                    
    except FileNotFoundError:
        print("Warning: WP_team_name_mappings.csv not found")
    except Exception as e:
        print(f"Error loading team mappings: {e}")
    
    return mappings, canonical_names

# Load mappings at startup
TEAM_MAPPINGS, CANONICAL_NAMES = load_team_mappings()
print(f"Loaded {len(TEAM_MAPPINGS)} team name mappings")

def normalize_team_name(team_name):
    """Convert team name to standardized form using mappings"""
    team_name = team_name.strip()
    
    # Direct mapping
    if team_name in TEAM_MAPPINGS:
        team_id = TEAM_MAPPINGS[team_name]
        return CANONICAL_NAMES.get(team_id, team_name)
    
    # Case-insensitive mapping
    team_name_lower = team_name.lower()
    for mapped_name, team_id in TEAM_MAPPINGS.items():
        if mapped_name.lower() == team_name_lower:
            return CANONICAL_NAMES.get(team_id, team_name)
    
    # Return original if no mapping found
    return team_name

def find_teams_in_rankings(target_teams):
    """Find all team names in rankings data that match the target teams"""
    matched_teams = set()
    target_team_ids = set()
    
    # Get team IDs for target teams
    for team in target_teams:
        normalized = normalize_team_name(team)
        if normalized in TEAM_MAPPINGS:
            target_team_ids.add(TEAM_MAPPINGS[normalized])
    
    # Find all team names in rankings that belong to these team IDs
    for date_str, ranking_list in rankings.items():
        for team_entry in ranking_list:
            team_name = team_entry['team_name']
            normalized = normalize_team_name(team_name)
            if normalized in TEAM_MAPPINGS:
                team_id = TEAM_MAPPINGS[normalized]
                if team_id in target_team_ids:
                    matched_teams.add(team_name)
    
    return list(matched_teams)

# Cache management functions
def cache_key_generator(*args):
    """Generate a cache key from arguments"""
    key_string = "_".join(str(arg) for arg in args)
    return hashlib.md5(key_string.encode()).hexdigest()

def get_from_cache(key):
    """Get data from cache if it exists and is not expired"""
    if key in CACHE:
        data, timestamp = CACHE[key]
        if time.time() - timestamp < CACHE_TTL:
            return data
        else:
            # Remove expired entry
            del CACHE[key]
    return None

def set_cache(key, data):
    """Set data in cache with timestamp"""
    # Implement simple LRU by removing oldest entries if cache is full
    if len(CACHE) >= CACHE_MAX_SIZE:
        # Remove the oldest entry
        oldest_key = min(CACHE.keys(), key=lambda k: CACHE[k][1])
        del CACHE[oldest_key]
    
    CACHE[key] = (data, time.time())

def add_cache_headers(response, max_age=3600):
    """Add cache headers to response"""
    response.headers['Cache-Control'] = f'public, max-age={max_age}, s-maxage={max_age}, stale-while-revalidate=7200'
    response.headers['ETag'] = hashlib.md5(response.get_data()).hexdigest()
    return response

def fetch_collection_as_aligned_list(collection, is_float):
    # 1) Fetch all documents (exclude _id)
    docs = list(collection.find({}, {"_id": 0}))
    
    # 2) Build a map whose keys are always the string version of Rank:
    doc_map = {}
    for doc in docs:
        raw_rank = doc.get("Rank")
        # Convert to lowercase string, trimmed of whitespace:
        normalized_rank = str(raw_rank).strip().lower()
        doc_map[normalized_rank] = doc
    
    aligned = []
    for rank in RANK_ORDER: # RANK_ORDER is ["1","2",...,"20","unranked"]
        doc = doc_map.get(rank)
        if doc is None:
            # If a rank is missing, create empty row with nulls
            clean_row = {"rank": rank}
            for header in RANK_ORDER:
                clean_row[header] = None
            aligned.append(clean_row)
            continue
        
        clean_row = {"rank": rank}
        for key, val in doc.items():
            if key == "Rank":
                continue
            if val is None or val == "":
                clean_row[key] = None
            else:
                clean_row[key] = float(val) if is_float else int(val)
        aligned.append(clean_row)
    
    return aligned

@app.route("/api/matrix", methods=["GET"])
def get_matrix():
    try:
        # Check cache first
        cache_key = cache_key_generator("matrix", "v1")
        cached_data = get_from_cache(cache_key)
        
        if cached_data:
            print("Serving matrix data from cache")
            response = make_response(jsonify(cached_data))
            return add_cache_headers(response), 200
        
        print("Fetching matrix data from database")
        delim_data = fetch_collection_as_aligned_list(delim_col, is_float=False) # Game counts
        prob_data = fetch_collection_as_aligned_list(prob_col,is_float=True)
        
        headers = RANK_ORDER.copy() # ["1","2",...,"20","unranked"]
        
        result_data = {
            "headers": headers,
            "probData": prob_data, # Changed from "winData" to "probData"
            "delimData": delim_data
        }
        
        # Cache the result
        set_cache(cache_key, result_data)
        
        print(f"Returning {len(prob_data)} probability rows and {len(delim_data)} delim rows")
        response = make_response(jsonify(result_data))
        return add_cache_headers(response), 200
        
    except Exception as e:
        print(f"Error in /api/matrix: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/matches/<row_rank>/<col_rank>", methods=["GET"])
def get_matches(row_rank, col_rank):
    """Get matches between two specific ranks"""
    try:
        # Check cache first
        cache_key = cache_key_generator("matches", row_rank, col_rank)
        cached_data = get_from_cache(cache_key)
        
        if cached_data:
            print(f"Serving matches data for {row_rank}_{col_rank} from cache")
            response = make_response(jsonify(cached_data))
            return add_cache_headers(response), 200
        
        print(f"Fetching matches data for {row_rank}_{col_rank} from database")
        
        # Create the key for matches lookup
        # Try both directions since matches can be stored as "3_9" or "9_3"
        key1 = f"{int(row_rank)-1}_{int(col_rank)-1}"

        # Look for matches document
        matches_doc = matches_col.find({},{"_id": 0})
        games = list(matches_doc)[0][key1]

        result_data = {
            "matches": games,
            "count": len(games),
            "row_rank": row_rank,
            "col_rank": col_rank
        }
        
        # Cache the result
        set_cache(cache_key, result_data)
        
        response = make_response(jsonify(result_data))
        return add_cache_headers(response), 200
        
    except Exception as e:
        print(f"Error in /api/matches/{row_rank}/{col_rank}: {e}")
        return jsonify({"error": str(e)}), 500
    

@app.route("/rankings/<team_names>/<start_date>/<end_date>", methods=["GET"])
def get_team_ranking_history(team_names, start_date, end_date):
    try:
        # Check cache first
        cache_key = cache_key_generator("rankings", team_names, start_date, end_date)
        cached_data = get_from_cache(cache_key)
        
        if cached_data:
            print(f"Serving ranking history for {team_names} from cache")
            response = make_response(jsonify(cached_data))
            return add_cache_headers(response), 200
        
        print(f"Fetching ranking history for {team_names} from database")
        
        # Convert string dates to datetime objects for comparison
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        # Parse team names (comma-separated)
        team_list = [name.strip() for name in team_names.split(',')]
        
        history = []
        for date_str, ranking_list in rankings.items():
            # Parse date string to datetime object
            current_date = datetime.strptime(date_str.split('-')[0], "%m/%d/%Y")
            
            # Check if current_date is within the specified range
            if start_dt <= current_date <= end_dt:
                for team in ranking_list:
                    if team['team_name'] in team_list:
                        history.append({
                            "team_name": team['team_name'],
                            "date": current_date.strftime("%Y-%m-%d"),
                            "rank": team['ranking']
                        })
        
        # Sort by date and team name
        history.sort(key=lambda x: (x['date'], x['team_name']))
        
        result_data = {
            "data": history,
            "count": len(history),
            "teams": team_list,
            "date_range": {
                "start": start_date,
                "end": end_date
            }
        }
        
        # Cache the result
        set_cache(cache_key, result_data)
        
        response = make_response(jsonify(result_data))
        return add_cache_headers(response), 200
        
    except Exception as e:
        print(f"Error in /rankings/{team_names}/{start_date}/{end_date}: {e}")
        return jsonify({"error": str(e)}), 500



@app.route("/api/health", methods=["GET"])
def health_check():
    """Simple health check endpoint"""
    return jsonify({"status": "healthy", "message": "Flask server is running"}), 200

@app.route("/api/cache/info", methods=["GET"])
def cache_info():
    """Get cache statistics"""
    cache_stats = {
        "cache_size": len(CACHE),
        "max_cache_size": CACHE_MAX_SIZE,
        "cache_ttl_seconds": CACHE_TTL,
        "cached_keys": list(CACHE.keys()) if len(CACHE) < 20 else f"{len(CACHE)} keys (too many to list)"
    }
    return jsonify(cache_stats), 200

@app.route("/api/cache/clear", methods=["POST"])
def clear_cache():
    """Clear all cache entries"""
    global CACHE
    old_size = len(CACHE)
    CACHE.clear()
    return jsonify({
        "message": f"Cache cleared successfully. Removed {old_size} entries.",
        "cache_size": len(CACHE)
    }), 200

def warm_cache():
    """Pre-populate cache with commonly requested data"""
    print("Warming up cache...")
    
    try:
        # Warm up matrix data
        cache_key = cache_key_generator("matrix", "v1")
        if not get_from_cache(cache_key):
            delim_data = fetch_collection_as_aligned_list(delim_col, is_float=False)
            prob_data = fetch_collection_as_aligned_list(prob_col, is_float=True)
            headers = RANK_ORDER.copy()
            
            result_data = {
                "headers": headers,
                "probData": prob_data,
                "delimData": delim_data
            }
            set_cache(cache_key, result_data)
            print("Matrix data cached")
        
        # Warm up some common matches (rank 1-5 vs rank 1-5)
        for i in range(1, 6):
            for j in range(1, 6):
                if i != j:  # Don't cache same rank vs same rank
                    cache_key = cache_key_generator("matches", str(i), str(j))
                    if not get_from_cache(cache_key):
                        try:
                            key1 = f"{i-1}_{j-1}"
                            matches_doc = matches_col.find({}, {"_id": 0})
                            games = list(matches_doc)[0][key1]
                            
                            result_data = {
                                "matches": games,
                                "count": len(games),
                                "row_rank": str(i),
                                "col_rank": str(j)
                            }
                            set_cache(cache_key, result_data)
                        except:
                            pass  # Skip if data doesn't exist
        
        print(f"Cache warming completed. Cache size: {len(CACHE)}")
        
    except Exception as e:
        print(f"Error warming cache: {e}")

# Warm cache on startup
def startup():
    warm_cache()

if __name__ == "__main__":
    # Warm cache before starting the server
    startup()
    # When you run locally: python app.py → listens on http://127.0.0.1:5001
    app.run(host="0.0.0.0", port=5001, debug=True)
    

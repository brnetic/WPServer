#!/usr/bin/env python3
"""
Transform men's water polo rankings JSON file by replacing team names with team IDs
based on the team name mappings CSV file.
"""

import json
import csv
import sys
from pathlib import Path


def load_team_mappings(csv_file_path):
    """Load team name mappings from CSV file"""
    mappings = {}
    
    try:
        with open(csv_file_path, 'r', encoding='utf-8-sig') as f:  # Handle BOM
            reader = csv.DictReader(f)
            for row in reader:
                team_name = row['team_name'].strip()
                team_id = int(row['team_id'])
                mappings[team_name] = team_id
        
        print(f"Loaded {len(mappings)} team mappings from {csv_file_path}")
        return mappings
    
    except FileNotFoundError:
        print(f"Error: CSV file {csv_file_path} not found")
        sys.exit(1)
    except Exception as e:
        print(f"Error loading CSV file: {e}")
        sys.exit(1)


def normalize_team_name(team_name):
    """Normalize team name by removing common variations"""
    if not team_name:
        return team_name
    
    # Clean up the team name
    normalized = team_name.strip()
    
    # Handle common variations
    replacements = {
        "University of California, Berkeley": "University of California",
        "University of California, Los Angeles": "University of California-Los Angeles",
        "University of California, Irvine": "University of California-Irvine",
        "University of California, Santa Barbara": "University of California-Santa Barbara",
        "University of California, Davis": "University of California-Davis",
        "University of California, San Diego": "University of California-San Diego",
        "St. Francis College (NY)": "Saint Francis",
        "St. Francis": "Saint Francis",
        "California Baptist University": "California Baptist University",
        "Cal Baptist": "California Baptist",
        "CBU": "California Baptist University",
    }
    
    for old_name, new_name in replacements.items():
        if normalized == old_name:
            normalized = new_name
            break
    
    return normalized


def find_team_id(team_name, mappings):
    """Find team ID for a given team name, trying various matching strategies"""
    if not team_name:
        return None
    
    # Try exact match first
    if team_name in mappings:
        return mappings[team_name]
    
    # Try normalized name
    normalized = normalize_team_name(team_name)
    if normalized in mappings:
        return mappings[normalized]
    
    # Try case-insensitive match
    team_name_lower = team_name.lower()
    for mapped_name, team_id in mappings.items():
        if mapped_name.lower() == team_name_lower:
            return team_id
    
    # Try partial matches for common variations
    for mapped_name, team_id in mappings.items():
        # Check if the team name contains key parts of the mapped name
        if "USC" in team_name and "USC" in mapped_name:
            return team_id
        elif "UCLA" in team_name and "UCLA" in mapped_name:
            return team_id
        elif "Stanford" in team_name and "Stanford" in mapped_name:
            return team_id
        elif "Berkeley" in team_name and ("UC Berkeley" in mapped_name or "California" in mapped_name):
            return team_id
        elif "Irvine" in team_name and "Irvine" in mapped_name:
            return team_id
        elif "Santa Barbara" in team_name and ("UCSB" in mapped_name or "Santa Barbara" in mapped_name):
            return team_id
        elif "Davis" in team_name and "Davis" in mapped_name:
            return team_id
        elif "San Diego" in team_name and "San Diego" in mapped_name:
            return team_id
        elif "Pepperdine" in team_name and "Pepperdine" in mapped_name:
            return team_id
        elif "Loyola" in team_name and ("LMU" in mapped_name or "Loyola" in mapped_name):
            return team_id
        elif "Long Beach" in team_name and "Long Beach" in mapped_name:
            return team_id
        elif "Naval Academy" in team_name and ("Navy" in mapped_name or "Naval" in mapped_name):
            return team_id
        elif "Francis" in team_name and "Francis" in mapped_name:
            return team_id
    
    return None


def transform_rankings(input_file, output_file, mappings):
    """Transform rankings JSON by replacing team names with IDs"""
    
    try:
        # Load the rankings JSON
        with open(input_file, 'r', encoding='utf-8') as f:
            rankings = json.load(f)
        
        print(f"Loaded rankings from {input_file}")
        
        # Transform the data
        transformed_rankings = {}
        total_teams = 0
        mapped_teams = 0
        unmapped_teams = set()
        
        for date_range, team_list in rankings.items():
            transformed_list = []
            
            for team_entry in team_list:
                team_name = team_entry.get('team_name', '')
                ranking = team_entry.get('ranking')
                
                if team_name:
                    total_teams += 1
                    team_id = find_team_id(team_name, mappings)
                    
                    if team_id is not None:
                        # Replace team_name with team_id
                        transformed_entry = {
                            'team_id': team_id,
                            'ranking': ranking
                        }
                        mapped_teams += 1
                    else:
                        # Keep original team_name if no mapping found
                        transformed_entry = {
                            'team_name': team_name,
                            'ranking': ranking
                        }
                        unmapped_teams.add(team_name)
                    
                    transformed_list.append(transformed_entry)
            
            transformed_rankings[date_range] = transformed_list
        
        # Save the transformed data
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(transformed_rankings, f, indent=2, ensure_ascii=False)
        
        print(f"Transformed rankings saved to {output_file}")
        print(f"Statistics:")
        print(f"  Total teams processed: {total_teams}")
        print(f"  Teams mapped to IDs: {mapped_teams}")
        print(f"  Teams not mapped: {len(unmapped_teams)}")
        
        if unmapped_teams:
            print(f"\nUnmapped team names:")
            for team in sorted(unmapped_teams):
                print(f"  - {team}")
        
    except FileNotFoundError:
        print(f"Error: Input file {input_file} not found")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in input file: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error transforming rankings: {e}")
        sys.exit(1)


def main():
    """Main function"""
    # File paths
    script_dir = Path(__file__).parent
    csv_file = script_dir.parent / "WP_team_name_mappings.csv"
    input_file = script_dir / "mens_waterpolo_rankings.json"
    output_file = script_dir / "mens_waterpolo_rankings_with_ids.json"
    
    print("Water Polo Rankings Transformer")
    print("================================")
    print(f"CSV mappings file: {csv_file}")
    print(f"Input JSON file: {input_file}")
    print(f"Output JSON file: {output_file}")
    print()
    
    # Load team mappings
    mappings = load_team_mappings(csv_file)
    
    # Transform rankings
    transform_rankings(input_file, output_file, mappings)
    
    print("\nTransformation complete!")


if __name__ == "__main__":
    main()

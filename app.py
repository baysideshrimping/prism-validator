"""
PRISM Validator - CDC Seasonal Vaccination Aggregate Data Validation
Validates COVID, Flu, and RSV aggregate reporting submissions
"""

from flask import Flask, render_template, request, jsonify, redirect, url_for, send_file
import pandas as pd
import numpy as np
import os
import json
import uuid
import re
from datetime import datetime
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'data/uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max

# Ensure upload directory exists
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs('data', exist_ok=True)

# Data storage
DATA_FILE = 'data/submissions.json'
CONFIG_FILE = 'data/config.json'

# Default expected submission period (can be changed via admin)
DEFAULT_EXPECTED_YEAR = 2026
DEFAULT_EXPECTED_MONTH = 'JAN'

def load_config():
    """Load configuration including expected submission month"""
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {
        'expected_year': DEFAULT_EXPECTED_YEAR,
        'expected_month': DEFAULT_EXPECTED_MONTH
    }

def save_config(config):
    """Save configuration"""
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=2)

# =============================================================================
# TEMPLATE DEFINITIONS
# =============================================================================

COVID_COLUMNS = [
    'Season', 'Month', 'Vax date',
    '6 months-23 months DOB range', '6 months-23 months numerator', '6 months-23 months population',
    '2-4 years DOB range', '2-4 years numerator', '2-4 years population',
    '5-8 years DOB range', '5-8 years numerator', '5-8 years population',
    '9-12 years DOB range', '9-12 years numerator', '9-12 years population',
    '13-17 years DOB range', '13-17 years numerator', '13-17 years population',
    '18-49 years DOB range', '18-49 years numerator', '18-49 years population',
    '50-64 years DOB range ', '50-64 years numerator', '50-64 years population',
    '65+ years DOB range ', '65+ years numerator', '65+ years population',
    '6 months to 17 years DOB range', '6 months to 17 years numerator', '6 months to 17 years population',
    'Overall (all ages) DOB range', 'Overall numerator (all ages)', 'Overall (all ages) population',
    'All adults (+18)  DOB range ', 'All adults numerator (+18)', 'All adults (+18) population',
    'Report Due Date '
]

FLU_COLUMNS = [
    'Flu Season', 'Month', 'Vax date',
    '6 months-23 months  DOB range', '6 months-23 months numerator', '6 months-23 months population',
    '2-4 years DOB range', '2-4 years numerator', '2-4 years population',
    '5-8 years DOB range', '5-8 years numerator', '5-8 years population',
    '9-12 years DOB range', '9-12 years numerator', '9-12 years population',
    '13-17 years DOB range', '13-17 years numerator', '13-17 years population',
    '18-49 years DOB range', '18-49 years numerator', '18-49 years population',
    '50-64 years DOB range ', '50-64 years numerator', '50-64 years population',
    '65+ years DOB range ', '65+ years numerator', '65+ years population',
    '6 months to 17 years DOB range', '6 months to 17 years numerator', '6 months to 17 years population',
    'All adults (+18)  DOB range ', 'All adults numerator (+18)', 'All adults (+18) population',
    'Overall (all ages) DOB range', 'Overall numerator (all ages)', 'Overall (all ages) population',
    'Report Due Date '
]

RSV_COLUMNS = [
    'RSV Season', 'Month',
    'vax_date_0-7 months', 'vax_date_8-19 months', 'vax_date_60+ years',
    '0 months-7 months DOB range', '0 months-7 months numerator', '0 months-7 months population',
    '8 months-19 months DOB range', '8 months-19 months numerator', '8 months-19 months population',
    '50-59 years DOB range ', '50-59 years numerator', '50-59 years population',
    '60-74 years DOB range ', '60-74 years numerator', '60-74 years population',
    '75+ years DOB range ', '75+ years numerator', '75+ years population',
    '60+ years DOB range ', '60+ years numerator', '60+ years population',
    'Report Due Date '
]

VALID_MONTHS = ['Jul', 'Aug', 'Sept', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun']

# Valid 3-letter site codes (IIS grantee codes)
VALID_SITE_CODES = {
    'AKA': 'Alaska', 'ALA': 'Alabama', 'ARA': 'Arkansas', 'ASA': 'American Samoa',
    'AZA': 'Arizona', 'BAA': 'NYC', 'CAA': 'California', 'CHA': 'Chicago',
    'COA': 'Colorado', 'CTA': 'Connecticut', 'DCA': 'DC', 'DEA': 'Delaware',
    'FLA': 'Florida', 'FMA': 'Micronesia', 'GAA': 'Georgia', 'GUA': 'Guam',
    'HIA': 'Hawaii', 'IAA': 'Iowa', 'IDA': 'Idaho', 'ILA': 'Illinois',
    'INA': 'Indiana', 'KSA': 'Kansas', 'KYA': 'Kentucky', 'LAA': 'Louisiana',
    'MAA': 'Massachusetts', 'MDA': 'Maryland', 'MEA': 'Maine', 'MHA': 'Marshall Islands',
    'MIA': 'Michigan', 'MNA': 'Minnesota', 'MOA': 'Missouri', 'MPA': 'N Mariana Islands',
    'MSA': 'Mississippi', 'MTA': 'Montana', 'NCA': 'North Carolina', 'NDA': 'North Dakota',
    'NEA': 'Nebraska', 'NHA': 'New Hampshire', 'NJA': 'New Jersey', 'NMA': 'New Mexico',
    'NVA': 'Nevada', 'NYA': 'New York State', 'OHA': 'Ohio', 'OKA': 'Oklahoma',
    'ORA': 'Oregon', 'PAA': 'Pennsylvania', 'PHA': 'Philadelphia', 'PRA': 'Puerto Rico',
    'RIA': 'Rhode Island', 'RPA': 'Palau', 'SCA': 'South Carolina', 'SDA': 'South Dakota',
    'TBA': 'San Antonio', 'THA': 'Houston', 'TNA': 'Tennessee', 'TXA': 'Texas',
    'UTA': 'Utah', 'VAA': 'Virginia', 'VIA': 'Virgin Islands', 'VTA': 'Vermont',
    'WAA': 'Washington State', 'WIA': 'Wisconsin', 'WVA': 'West Virginia', 'WYA': 'Wyoming'
}

# 3-letter month abbreviations for filenames
FILENAME_MONTHS = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']

# Map filename months to data months
MONTH_MAP = {
    'JAN': 'Jan', 'FEB': 'Feb', 'MAR': 'Mar', 'APR': 'Apr', 'MAY': 'May', 'JUN': 'Jun',
    'JUL': 'Jul', 'AUG': 'Aug', 'SEP': 'Sept', 'OCT': 'Oct', 'NOV': 'Nov', 'DEC': 'Dec'
}

# Age group definitions for rollup validation
COVID_FLU_CHILD_GROUPS = ['6 months-23 months', '2-4 years', '5-8 years', '9-12 years', '13-17 years']
COVID_FLU_ADULT_GROUPS = ['18-49 years', '50-64 years', '65+ years']
RSV_ADULT_60_PLUS = ['60-74 years', '75+ years']

# =============================================================================
# VALIDATION RESULT CLASS
# =============================================================================

class ValidationResult:
    def __init__(self, filename):
        self.filename = filename
        self.submission_id = str(uuid.uuid4())[:8]
        self.timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.status = 'pending'
        self.report_type = None  # COVID, FLU, or RSV
        self.errors = []
        self.row_count = 0
        self.season = None

    def add_error(self, row, field, message):
        self.errors.append({
            'row': row,
            'field': field,
            'message': message
        })

    def to_dict(self):
        return {
            'submission_id': self.submission_id,
            'filename': self.filename,
            'timestamp': self.timestamp,
            'status': self.status,
            'report_type': self.report_type,
            'row_count': self.row_count,
            'error_count': len(self.errors),
            'errors': self.errors,
            'season': self.season
        }

# =============================================================================
# DATA PERSISTENCE
# =============================================================================

def load_submissions():
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r') as f:
                return json.load(f)
        except:
            return []
    return []

def save_submission(result):
    submissions = load_submissions()
    submissions.insert(0, result.to_dict())
    with open(DATA_FILE, 'w') as f:
        json.dump(submissions, f, indent=2)

def clear_all_data():
    if os.path.exists(DATA_FILE):
        os.remove(DATA_FILE)
    # Clear uploaded files
    upload_folder = app.config['UPLOAD_FOLDER']
    if os.path.exists(upload_folder):
        for f in os.listdir(upload_folder):
            os.remove(os.path.join(upload_folder, f))

# =============================================================================
# TEMPLATE DETECTION
# =============================================================================

def detect_template_type(df):
    """Detect if file is COVID, Flu, or RSV based on columns"""
    columns = [c.strip() for c in df.columns.tolist()]

    # Check for RSV first (most distinct)
    if 'RSV Season' in columns or 'vax_date_0-7 months' in columns:
        return 'RSV'

    # Check for Flu
    if 'Flu Season' in columns:
        return 'FLU'

    # Check for COVID
    if 'Season' in columns and 'Flu Season' not in columns:
        return 'COVID'

    return None

def get_expected_columns(report_type):
    if report_type == 'COVID':
        return COVID_COLUMNS
    elif report_type == 'FLU':
        return FLU_COLUMNS
    elif report_type == 'RSV':
        return RSV_COLUMNS
    return []

# =============================================================================
# FILENAME VALIDATION
# =============================================================================

def validate_filename(filename, result, df):
    """
    Validate filename follows expected CDC PRISM format:
    MonthlyAllCOVID_{SITE}_{YYYYMON}.csv
    MonthlyFlu_{SITE}_{YYYYMON}.csv
    MonthlyRSV_{SITE}_{YYYYMON}.csv

    Examples:
    - MonthlyAllCOVID_BAA_2024JAN.csv
    - MonthlyFlu_GAA_2025FEB.csv
    - MonthlyRSV_NCA_2024JUL.csv
    """
    # Get expected submission period from config
    config = load_config()
    expected_year = config.get('expected_year', DEFAULT_EXPECTED_YEAR)
    expected_month = config.get('expected_month', DEFAULT_EXPECTED_MONTH)

    # Remove .csv extension
    name = filename.replace('.csv', '').replace('.CSV', '')

    # Expected patterns for each type
    covid_pattern = r'^MonthlyAllCOVID_([A-Z]{3})_(\d{4})([A-Z]{3})$'
    flu_pattern = r'^MonthlyFlu_([A-Z]{3})_(\d{4})([A-Z]{3})$'
    rsv_pattern = r'^MonthlyRSV_([A-Z]{3})_(\d{4})([A-Z]{3})$'

    match = None
    detected_type = None

    if re.match(covid_pattern, name, re.IGNORECASE):
        match = re.match(covid_pattern, name, re.IGNORECASE)
        detected_type = 'COVID'
    elif re.match(flu_pattern, name, re.IGNORECASE):
        match = re.match(flu_pattern, name, re.IGNORECASE)
        detected_type = 'FLU'
    elif re.match(rsv_pattern, name, re.IGNORECASE):
        match = re.match(rsv_pattern, name, re.IGNORECASE)
        detected_type = 'RSV'

    if not match:
        # Detect specific issues with the filename
        issues = _detect_filename_issues(filename)

        # Build a helpful suggestion based on what we know
        suggested_filename = _suggest_filename(filename, result.report_type, expected_year, expected_month)

        # Create error message
        error_msg = f'Incorrect filename format. You uploaded: "{filename}". '

        if issues:
            error_msg += 'Issues found: ' + ' '.join(issues) + ' '

        error_msg += f'Currently accepting: {expected_year} {expected_month} submissions. '
        error_msg += f'Correct format: {suggested_filename}'

        result.add_error(0, 'filename', error_msg)
        return

    site_code, year, month = match.groups()
    site_code = site_code.upper()
    month = month.upper()

    # Validate site code
    if site_code not in VALID_SITE_CODES:
        result.add_error(0, 'filename',
            f'Invalid site code: "{site_code}". Use your 3-letter IIS grantee code (e.g., GAA for Georgia, NYA for New York)')

    # Validate type matches detected template type
    if result.report_type and detected_type != result.report_type:
        prefix = _get_filename_prefix(result.report_type)
        result.add_error(0, 'filename',
            f'Filename says {detected_type} but data looks like {result.report_type}. '
            f'Rename to: {prefix}_{site_code}_{expected_year}{expected_month}.csv')

    # Validate year is reasonable
    try:
        year_int = int(year)
        if year_int < 2020 or year_int > 2030:
            result.add_error(0, 'filename', f'Year in filename ({year}) seems invalid')
    except:
        result.add_error(0, 'filename', f'Invalid year in filename: {year}')

    # Validate month (3-letter format)
    if month not in FILENAME_MONTHS:
        result.add_error(0, 'filename',
            f'Invalid month: "{month}". Use 3-letter format: JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DEC')
        return

    # Check against expected submission period
    if year != str(expected_year) or month != expected_month:
        prefix = _get_filename_prefix(detected_type)
        result.add_error(0, 'filename',
            f'Wrong submission period. You submitted {year} {month} data, but currently accepting {expected_year} {expected_month}. '
            f'Rename to: {prefix}_{site_code}_{expected_year}{expected_month}.csv')

    # Check if file contains data for the month specified in filename
    if 'Month' in df.columns:
        data_month = MONTH_MAP.get(month, month)  # Convert JUL -> Jul, SEP -> Sept
        months_in_data = [str(m).strip() for m in df['Month'].tolist() if pd.notna(m)]
        if data_month not in months_in_data:
            result.add_error(0, 'filename',
                f'Month in filename ({month}) not found in data. Data contains: {", ".join(months_in_data[:3])}...')


def _get_filename_prefix(report_type):
    """Get the correct filename prefix for a report type"""
    prefixes = {
        'COVID': 'MonthlyAllCOVID',
        'FLU': 'MonthlyFlu',
        'RSV': 'MonthlyRSV'
    }
    return prefixes.get(report_type, 'MonthlyAllCOVID')


def _detect_filename_issues(filename):
    """Detect specific issues with filename to give targeted advice"""
    issues = []
    name_upper = filename.upper()

    # Check for spaces in filename
    if ' ' in filename:
        issues.append('Filename contains spaces. Use underscores instead.')

    # Check for dashes where underscores expected
    if '-' in filename and '_' not in filename:
        issues.append('Use underscores (_) not dashes (-) to separate parts.')

    # Check for common wrong prefixes
    wrong_prefixes = [
        ('COVID_', 'Should start with "MonthlyAllCOVID_"'),
        ('MONTHLYCOVID_', 'Should be "MonthlyAllCOVID_" (note the "All")'),
        ('MONTHLY_COVID_', 'Should be "MonthlyAllCOVID_" (no space/underscore)'),
        ('FLU_', 'Should start with "MonthlyFlu_"'),
        ('MONTHLYFLU_', 'Use "MonthlyFlu_" (check capitalization)'),
        ('RSV_', 'Should start with "MonthlyRSV_"'),
        ('MONTHLYRSV_', 'Use "MonthlyRSV_" (check capitalization)'),
    ]
    for prefix, msg in wrong_prefixes:
        if name_upper.startswith(prefix):
            issues.append(msg)
            break

    # Check for "draft", "final", "copy", "v2", "(1)" etc.
    unwanted = ['DRAFT', 'FINAL', 'COPY', '(1)', '(2)', '_V2', '_V3', '_NEW', '_OLD', 'REVISED', 'UPDATED']
    for word in unwanted:
        if word in name_upper:
            issues.append(f'Remove "{word}" from filename. Submit clean filename without version markers.')

    # Check for wrong month formats
    wrong_months = [
        ('SEPT', 'Use "SEP" not "SEPT" for September'),
        ('JUNE', 'Use "JUN" not "JUNE" for June'),
        ('JULY', 'Use "JUL" not "JULY" for July'),
        ('JANUARY', 'Use "JAN" not "JANUARY"'),
        ('FEBRUARY', 'Use "FEB" not "FEBRUARY"'),
    ]
    for wrong, msg in wrong_months:
        if wrong in name_upper and wrong + 'E' not in name_upper:  # Avoid false positives
            issues.append(msg)

    # Check for 2-letter state codes
    import re
    two_letter_match = re.search(r'_([A-Z]{2})_', name_upper)
    if two_letter_match:
        state = two_letter_match.group(1)
        if state + 'A' in VALID_SITE_CODES:
            issues.append(f'Use 3-letter IIS code "{state}A" instead of 2-letter "{state}"')

    return issues


def _suggest_filename(original_filename, detected_type, expected_year, expected_month):
    """
    Try to suggest the correct filename based on what we can parse from the original
    and what we detected from the data content.
    """
    # Try to extract a site code from the original filename
    site_code = 'XXX'  # placeholder

    # Look for common state abbreviations or site codes in the filename
    upper_name = original_filename.upper()

    # Check for 3-letter codes first
    for code in VALID_SITE_CODES.keys():
        if code in upper_name:
            site_code = code
            break

    # If no 3-letter code found, try to find 2-letter state abbreviations and convert
    if site_code == 'XXX':
        state_to_site = {
            'GA': 'GAA', 'NY': 'NYA', 'CA': 'CAA', 'TX': 'TXA', 'FL': 'FLA',
            'NC': 'NCA', 'PA': 'PAA', 'IL': 'ILA', 'OH': 'OHA', 'MI': 'MIA',
            'AK': 'AKA', 'AL': 'ALA', 'AR': 'ARA', 'AZ': 'AZA', 'CO': 'COA',
            'CT': 'CTA', 'DE': 'DEA', 'HI': 'HIA', 'IA': 'IAA', 'ID': 'IDA',
            'IN': 'INA', 'KS': 'KSA', 'KY': 'KYA', 'LA': 'LAA', 'MA': 'MAA',
            'MD': 'MDA', 'ME': 'MEA', 'MN': 'MNA', 'MO': 'MOA', 'MS': 'MSA',
            'MT': 'MTA', 'NE': 'NEA', 'NH': 'NHA', 'NJ': 'NJA', 'NM': 'NMA',
            'NV': 'NVA', 'OK': 'OKA', 'OR': 'ORA', 'RI': 'RIA', 'SC': 'SCA',
            'SD': 'SDA', 'TN': 'TNA', 'UT': 'UTA', 'VA': 'VAA', 'VT': 'VTA',
            'WA': 'WAA', 'WI': 'WIA', 'WV': 'WVA', 'WY': 'WYA'
        }
        for state, code in state_to_site.items():
            # Look for state code with word boundary (underscore, start/end)
            if f'_{state}_' in upper_name or upper_name.startswith(f'{state}_') or f'_{state}.' in upper_name:
                site_code = code
                break

    # Get the right prefix based on detected type
    prefix = _get_filename_prefix(detected_type)

    return f'{prefix}_{site_code}_{expected_year}{expected_month}.csv'

# =============================================================================
# VALIDATION FUNCTIONS
# =============================================================================

def validate_prism_file(filepath, filename):
    """Main validation function for PRISM files"""
    result = ValidationResult(filename)

    # Try to read the file
    try:
        df = pd.read_csv(filepath, encoding='utf-8-sig')
    except Exception as e:
        try:
            df = pd.read_csv(filepath, encoding='latin-1')
        except Exception as e2:
            result.add_error(0, 'file', f'Cannot read file: {str(e2)}')
            result.status = 'failed'
            return result

    if df.empty:
        result.add_error(0, 'file', 'File is empty')
        result.status = 'failed'
        return result

    result.row_count = len(df)

    # Detect template type
    report_type = detect_template_type(df)
    if not report_type:
        result.add_error(0, 'file', 'Cannot detect template type. Expected COVID, Flu, or RSV template.')
        result.status = 'failed'
        return result

    result.report_type = report_type

    # Run all validations
    validate_filename(filename, result, df)
    validate_structure(df, result)
    validate_template_integrity(df, result)
    validate_numerator_population(df, result)
    validate_rollups(df, result)
    validate_cumulative_data(df, result)
    validate_data_quality(df, result)

    # Extract season for display
    season_col = 'RSV Season' if report_type == 'RSV' else ('Flu Season' if report_type == 'FLU' else 'Season')
    if season_col in df.columns:
        result.season = df[season_col].iloc[0] if len(df) > 0 else None

    # Set final status
    if len(result.errors) == 0:
        result.status = 'passed'
    else:
        result.status = 'failed'

    return result

# -----------------------------------------------------------------------------
# STRUCTURE VALIDATION
# -----------------------------------------------------------------------------

def validate_structure(df, result):
    """Validate file structure matches expected template"""
    expected_cols = get_expected_columns(result.report_type)
    actual_cols = df.columns.tolist()

    # First, check for old template versions
    old_template_check = detect_old_template(actual_cols, result.report_type)
    if old_template_check:
        result.add_error(0, 'template',
            f'Old template detected: {old_template_check}. Please download the current template from the Templates page.')
        return  # Don't continue with other structure checks

    # Check column count
    if len(actual_cols) != len(expected_cols):
        result.add_error(0, 'structure',
            f'Column count mismatch: expected {len(expected_cols)} columns, got {len(actual_cols)}. '
            f'Download the current template to see the correct format.')

    # Check for missing columns with typo detection
    actual_cols_stripped = [c.strip() for c in actual_cols]
    expected_cols_stripped = [c.strip() for c in expected_cols]

    for expected in expected_cols_stripped:
        if expected not in actual_cols_stripped:
            # Check for typos/close matches
            close_match = find_close_match(expected, actual_cols_stripped)
            if close_match:
                result.add_error(0, 'structure',
                    f'Column typo? Found "{close_match}" but expected "{expected}". Check spelling.')
            else:
                result.add_error(0, 'structure', f'Missing column: "{expected}"')

    # Check for extra columns (often Excel artifacts)
    for actual in actual_cols_stripped:
        if actual not in expected_cols_stripped:
            if actual.startswith('Unnamed:') or actual == '':
                result.add_error(0, 'structure',
                    f'Extra blank column detected. Delete empty columns before exporting from Excel.')
            else:
                close_match = find_close_match(actual, expected_cols_stripped)
                if close_match:
                    result.add_error(0, 'structure',
                        f'Unexpected column "{actual}" - did you mean "{close_match}"?')
                else:
                    result.add_error(0, 'structure',
                        f'Unexpected column: "{actual}". This column is not in the PRISM template.')

    # Check for blank rows
    blank_rows = df.isna().all(axis=1)
    for idx, is_blank in enumerate(blank_rows):
        if is_blank:
            result.add_error(idx + 2, 'structure',
                'Blank row detected. Delete empty rows before submitting.')

    # Check row count (should be 12 for a complete season)
    if len(df) > 0 and len(df) != 12:
        if len(df) > 12:
            result.add_error(0, 'structure',
                f'Too many rows: expected 12 months of data, got {len(df)} rows. '
                f'Remove extra rows (header row not counted).')
        elif len(df) < 12:
            result.add_error(0, 'structure',
                f'Only {len(df)} months of data found. A complete season has 12 months (Jul-Jun). '
                f'Partial submissions may be intentional if mid-season.')


def detect_old_template(columns, report_type):
    """Check if file appears to use an outdated template version"""

    # Known old column names from previous template versions
    old_covid_columns = [
        'COVID Season',  # Old name before standardization
        '6 months-4 years numerator',  # Old age grouping
        '5-11 years numerator',  # Old age grouping
        '12-17 years numerator',  # Old age grouping
    ]

    old_flu_columns = [
        'Season',  # Flu used to use generic "Season" in older versions
        '6 months-4 years numerator',
    ]

    old_rsv_columns = [
        '60-64 years numerator',  # RSV age groups changed
        '65-74 years numerator',
    ]

    cols_lower = [c.lower().strip() for c in columns]

    if report_type == 'COVID':
        for old_col in old_covid_columns:
            if old_col.lower() in cols_lower:
                return f'Found old column "{old_col}"'

    elif report_type == 'FLU':
        for old_col in old_flu_columns:
            if old_col.lower() in cols_lower:
                return f'Found old column "{old_col}"'

    elif report_type == 'RSV':
        for old_col in old_rsv_columns:
            if old_col.lower() in cols_lower:
                return f'Found old column "{old_col}"'

    return None


def find_close_match(target, candidates, threshold=0.8):
    """Find a close match using simple similarity"""

    target_lower = target.lower().strip()

    for candidate in candidates:
        candidate_lower = candidate.lower().strip()

        # Exact match (shouldn't happen but safety check)
        if target_lower == candidate_lower:
            return None

        # Check for common typos: missing space, extra space, swapped characters
        target_no_space = target_lower.replace(' ', '')
        candidate_no_space = candidate_lower.replace(' ', '')

        if target_no_space == candidate_no_space:
            return candidate

        # Check Levenshtein-style similarity (simple version)
        if len(target_lower) > 5 and len(candidate_lower) > 5:
            # Count matching characters
            matches = sum(1 for a, b in zip(target_lower, candidate_lower) if a == b)
            max_len = max(len(target_lower), len(candidate_lower))
            similarity = matches / max_len

            if similarity >= threshold:
                return candidate

        # Check if one contains the other (partial match)
        if len(target_lower) > 10 and len(candidate_lower) > 10:
            if target_lower in candidate_lower or candidate_lower in target_lower:
                return candidate

    return None

# -----------------------------------------------------------------------------
# TEMPLATE INTEGRITY VALIDATION
# -----------------------------------------------------------------------------

def validate_template_integrity(df, result):
    """Validate that pre-filled template fields weren't modified incorrectly"""

    # Get season column name
    if result.report_type == 'RSV':
        season_col = 'RSV Season'
    elif result.report_type == 'FLU':
        season_col = 'Flu Season'
    else:
        season_col = 'Season'

    # Validate season format (YYYY-YY)
    if season_col in df.columns:
        for idx, val in enumerate(df[season_col]):
            if pd.notna(val):
                val_str = str(val).strip()
                if not validate_season_format(val_str):
                    result.add_error(idx + 2, season_col,
                        f'Invalid season format: "{val_str}". Expected YYYY-YY (e.g., 2025-26)')

    # Validate month sequence
    if 'Month' in df.columns:
        months = df['Month'].tolist()
        for idx, month in enumerate(months):
            if pd.notna(month):
                month_str = str(month).strip()
                if month_str not in VALID_MONTHS:
                    result.add_error(idx + 2, 'Month',
                        f'Invalid month: "{month_str}". Expected one of: {", ".join(VALID_MONTHS)}')

        # Check for duplicate months
        valid_months = [str(m).strip() for m in months if pd.notna(m) and str(m).strip() in VALID_MONTHS]
        if len(valid_months) != len(set(valid_months)):
            result.add_error(0, 'Month', 'Duplicate months detected')

    # Validate Report Due Date format
    due_date_col = 'Report Due Date ' if 'Report Due Date ' in df.columns else 'Report Due Date'
    if due_date_col in df.columns:
        for idx, val in enumerate(df[due_date_col]):
            if pd.notna(val):
                if not validate_date_format(str(val)):
                    result.add_error(idx + 2, 'Report Due Date',
                        f'Invalid date format: "{val}"')

def validate_season_format(val):
    """Check if season is in YYYY-YY format with consecutive years"""
    import re
    match = re.match(r'^(\d{4})-(\d{2})$', str(val).strip())
    if not match:
        return False
    year1 = int(match.group(1))
    year2 = int(match.group(2))
    # Year2 should be last 2 digits of year1 + 1
    expected_year2 = (year1 + 1) % 100
    return year2 == expected_year2

def validate_date_format(val):
    """Check if date is in M/D/YYYY format"""
    import re
    # Accept M/D/YYYY or MM/DD/YYYY
    return bool(re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', str(val).strip()))

# -----------------------------------------------------------------------------
# NUMERATOR/POPULATION VALIDATION
# -----------------------------------------------------------------------------

def validate_numerator_population(df, result):
    """Validate numerator and population values"""

    # Get all numerator and population columns
    num_cols = [c for c in df.columns if 'numerator' in c.lower()]
    pop_cols = [c for c in df.columns if 'population' in c.lower()]

    for idx, row in df.iterrows():
        row_num = idx + 2  # Excel row number (1-indexed + header)

        for col in num_cols:
            val = row[col]
            if pd.notna(val):
                # Check if valid integer
                error = validate_integer_value(val, col)
                if error:
                    result.add_error(row_num, col, error)
                else:
                    # Check non-negative
                    try:
                        num_val = int(float(val))
                        if num_val < 0:
                            result.add_error(row_num, col, f'Negative value not allowed: {num_val}')
                        # Check reasonable magnitude
                        if num_val > 50000000:  # 50 million
                            result.add_error(row_num, col, f'Value suspiciously large: {num_val}')
                    except:
                        pass

        for col in pop_cols:
            val = row[col]
            if pd.notna(val):
                error = validate_integer_value(val, col)
                if error:
                    result.add_error(row_num, col, error)
                else:
                    try:
                        pop_val = int(float(val))
                        if pop_val < 0:
                            result.add_error(row_num, col, f'Negative value not allowed: {pop_val}')
                        if pop_val > 100000000:  # 100 million
                            result.add_error(row_num, col, f'Value suspiciously large: {pop_val}')
                    except:
                        pass

        # Check numerator <= population for paired columns
        validate_num_pop_pairs(row, row_num, result)

def validate_integer_value(val, field_name):
    """Validate that a value is a valid integer"""
    val_str = str(val).strip()

    # Check for placeholders
    placeholders = ['tbd', 'n/a', 'na', 'pending', 'xxx', 'test', '#n/a', '#ref!', '#value!', '#div/0!']
    if val_str.lower() in placeholders:
        return f'Placeholder text not allowed: "{val_str}"'

    # Check for formatted numbers with commas
    if ',' in val_str:
        return f'Remove comma formatting: "{val_str}"'

    # Check for scientific notation
    if 'e' in val_str.lower() and any(c.isdigit() for c in val_str):
        return f'Scientific notation not allowed: "{val_str}"'

    # Check for decimal
    try:
        float_val = float(val_str)
        if float_val != int(float_val):
            return f'Decimal not allowed, must be integer: "{val_str}"'
    except ValueError:
        return f'Invalid number: "{val_str}"'

    return None

def validate_num_pop_pairs(row, row_num, result):
    """Validate that numerator <= population for each age group"""

    # Define age group pairs
    age_groups = [
        ('6 months-23 months', '6 months-23 months numerator', '6 months-23 months population'),
        ('2-4 years', '2-4 years numerator', '2-4 years population'),
        ('5-8 years', '5-8 years numerator', '5-8 years population'),
        ('9-12 years', '9-12 years numerator', '9-12 years population'),
        ('13-17 years', '13-17 years numerator', '13-17 years population'),
        ('18-49 years', '18-49 years numerator', '18-49 years population'),
        ('50-64 years', '50-64 years numerator', '50-64 years population'),
        ('65+ years', '65+ years numerator', '65+ years population'),
        ('0 months-7 months', '0 months-7 months numerator', '0 months-7 months population'),
        ('8 months-19 months', '8 months-19 months numerator', '8 months-19 months population'),
        ('50-59 years', '50-59 years numerator', '50-59 years population'),
        ('60-74 years', '60-74 years numerator', '60-74 years population'),
        ('75+ years', '75+ years numerator', '75+ years population'),
        ('60+ years', '60+ years numerator', '60+ years population'),
        ('6 months to 17 years', '6 months to 17 years numerator', '6 months to 17 years population'),
        ('All adults (+18)', 'All adults numerator (+18)', 'All adults (+18) population'),
        ('Overall (all ages)', 'Overall numerator (all ages)', 'Overall (all ages) population'),
    ]

    for name, num_col, pop_col in age_groups:
        if num_col in row.index and pop_col in row.index:
            num_val = row[num_col]
            pop_val = row[pop_col]

            if pd.notna(num_val) and pd.notna(pop_val):
                try:
                    num = int(float(num_val))
                    pop = int(float(pop_val))

                    if num > pop:
                        result.add_error(row_num, num_col,
                            f'Numerator ({num:,}) exceeds population ({pop:,}) for {name}')

                    # Check vaccination rate isn't impossibly high
                    if pop > 0 and (num / pop) > 1.0:
                        result.add_error(row_num, num_col,
                            f'Vaccination rate exceeds 100% for {name}')
                except:
                    pass

            # Check paired: if one filled, other should be too
            if pd.notna(num_val) and pd.isna(pop_val):
                result.add_error(row_num, pop_col,
                    f'Population required when numerator is provided for {name}')

# -----------------------------------------------------------------------------
# ROLLUP VALIDATION
# -----------------------------------------------------------------------------

def validate_rollups(df, result):
    """Validate that rollup totals match sum of components"""

    for idx, row in df.iterrows():
        row_num = idx + 2

        if result.report_type in ['COVID', 'FLU']:
            # Child rollup: 6mo to 17y = sum of child age groups
            validate_rollup_sum(row, row_num, result,
                component_num_cols=['6 months-23 months numerator', '2-4 years numerator',
                                   '5-8 years numerator', '9-12 years numerator', '13-17 years numerator'],
                component_pop_cols=['6 months-23 months population', '2-4 years population',
                                   '5-8 years population', '9-12 years population', '13-17 years population'],
                rollup_num_col='6 months to 17 years numerator',
                rollup_pop_col='6 months to 17 years population',
                rollup_name='6 months to 17 years')

            # Adult rollup: All adults = sum of adult age groups
            validate_rollup_sum(row, row_num, result,
                component_num_cols=['18-49 years numerator', '50-64 years numerator', '65+ years numerator'],
                component_pop_cols=['18-49 years population', '50-64 years population', '65+ years population'],
                rollup_num_col='All adults numerator (+18)',
                rollup_pop_col='All adults (+18) population',
                rollup_name='All adults (+18)')

            # Overall rollup: all age groups
            all_num_cols = ['6 months-23 months numerator', '2-4 years numerator', '5-8 years numerator',
                           '9-12 years numerator', '13-17 years numerator', '18-49 years numerator',
                           '50-64 years numerator', '65+ years numerator']
            all_pop_cols = ['6 months-23 months population', '2-4 years population', '5-8 years population',
                           '9-12 years population', '13-17 years population', '18-49 years population',
                           '50-64 years population', '65+ years population']
            validate_rollup_sum(row, row_num, result,
                component_num_cols=all_num_cols,
                component_pop_cols=all_pop_cols,
                rollup_num_col='Overall numerator (all ages)',
                rollup_pop_col='Overall (all ages) population',
                rollup_name='Overall (all ages)')

        elif result.report_type == 'RSV':
            # RSV 60+ rollup
            validate_rollup_sum(row, row_num, result,
                component_num_cols=['60-74 years numerator', '75+ years numerator'],
                component_pop_cols=['60-74 years population', '75+ years population'],
                rollup_num_col='60+ years numerator',
                rollup_pop_col='60+ years population',
                rollup_name='60+ years')

def validate_rollup_sum(row, row_num, result, component_num_cols, component_pop_cols,
                        rollup_num_col, rollup_pop_col, rollup_name):
    """Check if rollup equals sum of components"""

    # Check numerator rollup
    if rollup_num_col in row.index:
        rollup_num = row[rollup_num_col]
        if pd.notna(rollup_num):
            try:
                rollup_val = int(float(rollup_num))
                component_sum = 0
                all_present = True

                for col in component_num_cols:
                    if col in row.index and pd.notna(row[col]):
                        component_sum += int(float(row[col]))
                    else:
                        all_present = False

                if all_present and rollup_val != component_sum:
                    result.add_error(row_num, rollup_num_col,
                        f'{rollup_name} numerator ({rollup_val:,}) does not equal sum of components ({component_sum:,})')
            except:
                pass

    # Check population rollup
    if rollup_pop_col in row.index:
        rollup_pop = row[rollup_pop_col]
        if pd.notna(rollup_pop):
            try:
                rollup_val = int(float(rollup_pop))
                component_sum = 0
                all_present = True

                for col in component_pop_cols:
                    if col in row.index and pd.notna(row[col]):
                        component_sum += int(float(row[col]))
                    else:
                        all_present = False

                if all_present and rollup_val != component_sum:
                    result.add_error(row_num, rollup_pop_col,
                        f'{rollup_name} population ({rollup_val:,}) does not equal sum of components ({component_sum:,})')
            except:
                pass

# -----------------------------------------------------------------------------
# CUMULATIVE DATA VALIDATION
# -----------------------------------------------------------------------------

def validate_cumulative_data(df, result):
    """Validate that numerators don't decrease (cumulative reporting)"""

    num_cols = [c for c in df.columns if 'numerator' in c.lower()]

    for col in num_cols:
        prev_val = None
        for idx, row in df.iterrows():
            val = row[col]
            if pd.notna(val):
                try:
                    curr_val = int(float(val))
                    if prev_val is not None and curr_val < prev_val:
                        result.add_error(idx + 2, col,
                            f'Cumulative value decreased from {prev_val:,} to {curr_val:,} (data should be cumulative)')
                    prev_val = curr_val
                except:
                    pass

def validate_population_stability(df, result):
    """Check that population doesn't swing wildly month-to-month"""

    pop_cols = [c for c in df.columns if 'population' in c.lower()]

    for col in pop_cols:
        values = []
        for idx, row in df.iterrows():
            val = row[col]
            if pd.notna(val):
                try:
                    values.append((idx, int(float(val))))
                except:
                    pass

        # Check for >20% swings
        for i in range(1, len(values)):
            prev_idx, prev_val = values[i-1]
            curr_idx, curr_val = values[i]

            if prev_val > 0:
                change_pct = abs(curr_val - prev_val) / prev_val * 100
                if change_pct > 20:
                    result.add_error(curr_idx + 2, col,
                        f'Population changed {change_pct:.1f}% from previous month (from {prev_val:,} to {curr_val:,})')

# -----------------------------------------------------------------------------
# DATA QUALITY VALIDATION
# -----------------------------------------------------------------------------

def validate_data_quality(df, result):
    """Check for data quality issues"""

    for idx, row in df.iterrows():
        row_num = idx + 2

        for col in df.columns:
            val = row[col]
            if pd.notna(val):
                val_str = str(val).strip()

                # Check for Excel errors
                excel_errors = ['#REF!', '#VALUE!', '#DIV/0!', '#NAME?', '#NULL!', '#N/A', '#NUM!']
                if val_str.upper() in excel_errors:
                    result.add_error(row_num, col, f'Excel error value: {val_str}. Fix the formula in your spreadsheet before exporting.')

                # Check for leading/trailing whitespace in values that matter
                if str(val) != str(val).strip():
                    if 'numerator' in col.lower() or 'population' in col.lower():
                        result.add_error(row_num, col, 'Value has leading/trailing whitespace')

                # Check for placeholder text in numeric fields
                if 'numerator' in col.lower() or 'population' in col.lower():
                    placeholders = ['tbd', 'n/a', 'na', 'pending', 'null', 'none', '-', '--', '...', 'xxx', 'x', '?', '??']
                    if val_str.lower() in placeholders:
                        result.add_error(row_num, col, f'Placeholder "{val_str}" found. Replace with actual data or 0 if no data available.')

                    # Check for text mixed with numbers
                    if any(c.isalpha() for c in val_str) and any(c.isdigit() for c in val_str):
                        if not val_str.replace('.','').replace('-','').replace('/','').replace(' ','').isdigit():
                            # Not a date format
                            if 'DOB' not in col and 'date' not in col.lower() and 'vax_date' not in col.lower():
                                result.add_error(row_num, col, f'Mixed text and numbers: "{val_str}". Enter numbers only.')

                    # Check for currency formatting
                    if val_str.startswith('$') or val_str.startswith('-$'):
                        result.add_error(row_num, col, f'Currency formatting not allowed: "{val_str}". Remove $ symbol.')

                    # Check for percentage signs
                    if '%' in val_str:
                        result.add_error(row_num, col, f'Percentage sign not allowed: "{val_str}". Enter raw numbers, not percentages.')

                    # Check for parentheses (accounting negative format)
                    if val_str.startswith('(') and val_str.endswith(')'):
                        result.add_error(row_num, col, f'Accounting format "(500)" detected: "{val_str}". Negative values are not valid for vaccination counts.')

                    # Check for "about", "approximately", "~"
                    approx_indicators = ['about', 'approx', 'approximately', '~', '>', '<', 'around', 'roughly', 'est', 'estimated']
                    for indicator in approx_indicators:
                        if indicator in val_str.lower():
                            result.add_error(row_num, col, f'Approximate values not allowed: "{val_str}". Enter exact counts from your IIS.')

    # Run additional smart checks
    validate_copy_paste_errors(df, result)
    validate_suspicious_patterns(df, result)
    validate_zero_logic(df, result)
    validate_season_mismatch(df, result)


def validate_copy_paste_errors(df, result):
    """Detect when data was likely copy-pasted across months without updating"""

    num_cols = [c for c in df.columns if 'numerator' in c.lower()]

    if len(df) < 3:
        return

    for col in num_cols:
        values = []
        for idx, row in df.iterrows():
            val = row[col]
            if pd.notna(val):
                try:
                    values.append(int(float(val)))
                except:
                    values.append(None)
            else:
                values.append(None)

        # Check for 3+ consecutive identical non-zero values
        consecutive_same = 1
        for i in range(1, len(values)):
            if values[i] is not None and values[i-1] is not None and values[i] == values[i-1] and values[i] > 0:
                consecutive_same += 1
                if consecutive_same >= 3:
                    # Extract age group name from column
                    age_group = col.replace(' numerator', '').replace(' numerator ', '')
                    result.add_error(0, col,
                        f'Possible copy-paste error: {age_group} has identical values ({values[i]:,}) for 3+ consecutive months. Cumulative data should increase monthly.')
                    break
            else:
                consecutive_same = 1


def validate_suspicious_patterns(df, result):
    """Detect suspicious data patterns that suggest errors"""

    num_cols = [c for c in df.columns if 'numerator' in c.lower()]
    pop_cols = [c for c in df.columns if 'population' in c.lower()]

    for idx, row in df.iterrows():
        row_num = idx + 2

        # Check for 100% vaccination rates (rare and suspicious)
        for num_col in num_cols:
            # Find matching population column
            base_name = num_col.replace('numerator', '').strip()
            pop_col = None
            for pc in pop_cols:
                if base_name in pc:
                    pop_col = pc
                    break

            if pop_col and num_col in row.index and pop_col in row.index:
                num_val = row[num_col]
                pop_val = row[pop_col]

                if pd.notna(num_val) and pd.notna(pop_val):
                    try:
                        num = int(float(num_val))
                        pop = int(float(pop_val))

                        if pop > 0 and num == pop and num > 100:
                            result.add_error(row_num, num_col,
                                f'Exactly 100% vaccination rate ({num:,}/{pop:,}) is unusual. Please verify this is correct.')
                    except:
                        pass

    # Check if ALL populations are round thousands (suggests estimates, not real IIS data)
    all_round = True
    pop_count = 0
    for col in pop_cols:
        for idx, row in df.iterrows():
            val = row[col]
            if pd.notna(val):
                try:
                    pop = int(float(val))
                    pop_count += 1
                    if pop > 0 and pop % 1000 != 0:
                        all_round = False
                        break
                except:
                    pass
        if not all_round:
            break

    if all_round and pop_count > 10:
        result.add_error(0, 'population',
            'All population values are round thousands. PRISM requires actual IIS population counts, not estimates.')


def validate_zero_logic(df, result):
    """Check for illogical zero values"""

    num_cols = [c for c in df.columns if 'numerator' in c.lower()]
    pop_cols = [c for c in df.columns if 'population' in c.lower()]

    for idx, row in df.iterrows():
        row_num = idx + 2

        # Check: population is 0 but numerator > 0 (impossible)
        for num_col in num_cols:
            base_name = num_col.replace('numerator', '').strip()
            pop_col = None
            for pc in pop_cols:
                if base_name in pc:
                    pop_col = pc
                    break

            if pop_col and num_col in row.index and pop_col in row.index:
                num_val = row[num_col]
                pop_val = row[pop_col]

                if pd.notna(num_val) and pd.notna(pop_val):
                    try:
                        num = int(float(num_val))
                        pop = int(float(pop_val))

                        if pop == 0 and num > 0:
                            result.add_error(row_num, pop_col,
                                f'Population is 0 but {num:,} vaccinations reported. Cannot vaccinate people who don\'t exist.')
                    except:
                        pass

        # Check: ALL numerators in a row are 0 (might be incomplete)
        all_zero = True
        has_data = False
        for col in num_cols:
            if col in row.index:
                val = row[col]
                if pd.notna(val):
                    has_data = True
                    try:
                        if int(float(val)) != 0:
                            all_zero = False
                            break
                    except:
                        all_zero = False

        if has_data and all_zero:
            month_col = 'Month' if 'Month' in row.index else None
            month_name = row[month_col] if month_col else f'Row {row_num}'
            result.add_error(row_num, 'numerator',
                f'All numerators are 0 for {month_name}. If no vaccinations occurred, this is correct. Otherwise, data may be missing.')


def validate_season_mismatch(df, result):
    """Check for mismatches between filename season and data season"""

    # Get season from data
    season_col = None
    if 'RSV Season' in df.columns:
        season_col = 'RSV Season'
    elif 'Flu Season' in df.columns:
        season_col = 'Flu Season'
    elif 'Season' in df.columns:
        season_col = 'Season'

    if season_col and len(df) > 0:
        data_season = str(df[season_col].iloc[0]).strip() if pd.notna(df[season_col].iloc[0]) else None

        if data_season:
            # Get expected season from config
            config = load_config()
            expected_year = config.get('expected_year', DEFAULT_EXPECTED_YEAR)
            expected_month = config.get('expected_month', DEFAULT_EXPECTED_MONTH)

            # Determine expected season based on year/month
            # Flu/RSV season runs Jul-Jun, so Jan 2026 would be 2025-26 season
            month_to_num = {'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                          'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12}
            month_num = month_to_num.get(expected_month, 1)

            if month_num >= 7:  # Jul-Dec
                expected_season = f"{expected_year}-{str(expected_year + 1)[2:]}"
            else:  # Jan-Jun
                expected_season = f"{expected_year - 1}-{str(expected_year)[2:]}"

            if data_season != expected_season:
                result.add_error(0, season_col,
                    f'Season mismatch: Data shows "{data_season}" but currently accepting {expected_year} {expected_month} submissions (season {expected_season}).')

# =============================================================================
# ROUTES
# =============================================================================

@app.route('/')
def index():
    return redirect(url_for('submit'))

@app.route('/submit', methods=['GET', 'POST'])
def submit():
    message = None
    error = None

    if request.method == 'POST':
        if 'files' not in request.files:
            error = 'No files selected'
        else:
            files = request.files.getlist('files')
            if not files or all(f.filename == '' for f in files):
                error = 'No files selected'
            else:
                success_count = 0
                fail_count = 0

                for file in files:
                    if file and file.filename:
                        filename = secure_filename(file.filename)
                        if filename.endswith('.csv'):
                            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                            file.save(filepath)

                            result = validate_prism_file(filepath, filename)
                            save_submission(result)

                            if result.status == 'passed':
                                success_count += 1
                            else:
                                fail_count += 1
                        else:
                            error = f'Invalid file type: {filename}. Only CSV files accepted.'

                if success_count > 0 or fail_count > 0:
                    message = f'Processed {success_count + fail_count} file(s): {success_count} passed, {fail_count} failed'

    return render_template('submit.html', message=message, error=error)

@app.route('/validation')
def validation_dashboard():
    submissions = load_submissions()

    # Calculate summary stats
    summary = {
        'total_submissions': len(submissions),
        'passed': sum(1 for s in submissions if s['status'] == 'passed'),
        'failed': sum(1 for s in submissions if s['status'] == 'failed'),
        'total_errors': sum(s['error_count'] for s in submissions),
        'covid_count': sum(1 for s in submissions if s.get('report_type') == 'COVID'),
        'flu_count': sum(1 for s in submissions if s.get('report_type') == 'FLU'),
        'rsv_count': sum(1 for s in submissions if s.get('report_type') == 'RSV'),
    }
    summary['pass_rate'] = round(summary['passed'] / summary['total_submissions'] * 100) if summary['total_submissions'] > 0 else 0

    # Aggregate errors by field
    error_types = {}
    for sub in submissions:
        for err in sub.get('errors', []):
            field = err.get('field', 'unknown')
            error_types[field] = error_types.get(field, 0) + 1

    return render_template('validation_dashboard.html',
                         submissions=submissions,
                         summary=summary,
                         error_types=error_types)

@app.route('/validation/<submission_id>')
def validation_detail(submission_id):
    submissions = load_submissions()
    submission = next((s for s in submissions if s['submission_id'] == submission_id), None)

    if not submission:
        return 'Submission not found', 404

    return render_template('validation_detail.html', submission=submission)

@app.route('/api/clear', methods=['POST'])
def api_clear():
    data = request.get_json() or {}
    password = data.get('password', '')

    if password != 'prism2024':
        return jsonify({'error': 'Incorrect password'}), 401

    clear_all_data()
    return jsonify({'success': True})

@app.route('/templates')
def templates_page():
    """Page to download blank templates"""
    config = load_config()
    return render_template('templates_download.html', config=config)

@app.route('/admin')
def admin_page():
    """Admin page to configure expected submission period"""
    config = load_config()
    return render_template('admin.html', config=config, months=FILENAME_MONTHS)

@app.route('/api/config', methods=['POST'])
def update_config():
    """Update expected submission period"""
    data = request.get_json() or {}
    password = data.get('password', '')

    if password != 'prism2024':
        return jsonify({'error': 'Incorrect password'}), 401

    config = load_config()

    if 'expected_year' in data:
        try:
            config['expected_year'] = int(data['expected_year'])
        except:
            return jsonify({'error': 'Invalid year'}), 400

    if 'expected_month' in data:
        month = data['expected_month'].upper()
        if month in FILENAME_MONTHS:
            config['expected_month'] = month
        else:
            return jsonify({'error': 'Invalid month'}), 400

    save_config(config)
    return jsonify({'success': True, 'config': config})

@app.route('/download/<template_type>')
def download_template(template_type):
    """Download a blank template CSV"""
    template_type = template_type.upper()

    if template_type == 'COVID':
        columns = COVID_COLUMNS
        filename = 'STATE_COVID_YYYY-YY_Mon.csv'
    elif template_type == 'FLU':
        columns = FLU_COLUMNS
        filename = 'STATE_FLU_YYYY-YY_Mon.csv'
    elif template_type == 'RSV':
        columns = RSV_COLUMNS
        filename = 'STATE_RSV_YYYY-YY_Mon.csv'
    else:
        return 'Invalid template type', 400

    # Create blank template with headers and 12 empty rows
    df = pd.DataFrame(columns=columns)
    # Add 12 empty rows for 12 months
    for i in range(12):
        df.loc[i] = [None] * len(columns)

    # Fill in month column
    if 'Month' in df.columns:
        df['Month'] = VALID_MONTHS

    # Save to temp file and send
    temp_path = os.path.join(app.config['UPLOAD_FOLDER'], f'template_{template_type}.csv')
    df.to_csv(temp_path, index=False)

    return send_file(temp_path, as_attachment=True, download_name=filename)

# =============================================================================
# MAIN
# =============================================================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    debug = os.environ.get('FLASK_DEBUG', 'true').lower() == 'true'
    app.run(host='0.0.0.0', port=port, debug=debug)

def translate_country(c):
    # Some country names don't correspond with
    # entries in the countries table. This dict is used to
    # manually translate these
    ct = {
        'Burma': 'Myanmar',
        'Kyrgyzstan': 'Kyrgyz Republic',
        'Micronesia': 'Micronesia, Fed. Sts.',
        'Congo (Kinshasa)': 'DR Congo',
        'Congo (Brazzaville)': 'Congo Republic',
        'Saint Vincent and the Grenadines': 'St. Vincent and the Grenadines',
        'Faroe Islands': 'Faeroe Islands',
        'Saint Kitts and Nevis': 'St. Kitts and Nevis',
        'Brunei': 'Brunei Darussalam',
        'Cape Verde': 'Cabo Verde',
        'Svalbard': 'Svalbard and Jan Mayen Islands',
        'East Timor': 'Timor-Leste',
        'Saint Lucia': 'St. Lucia',
        'Saint Pierre and Miquelon': 'St. Pierre and Miquelon',
        'Wallis and Futuna': 'Wallis and Futuna Islands',
        'Macau': 'Macao',
        'Saint Helena': 'St. Helena',

        'Ivory Coast': "Cote d'Ivoire",
        'Democratic Republic of Congo': 'DR Congo',
        'Democratic Republic of the Congo': 'DR Congo',
        'Republic of the Congo': 'Congo Republic',
        'Russian Federation': 'Russia',
        "Democratic People's Republic of Korea": 'North Korea',
        'Republic of Korea': 'South Korea',
        'ALASKA': 'United States',
        'Netherland': 'Netherlands',
        'UNited Kingdom': 'United Kingdom',
        'Somali Republic': 'Somalia',
        'Hong Kong SAR of China': 'Hong Kong',
        'Lao Peoples Democratic Republic': 'Laos',
        'Russia]]': 'Russia',
        'Syrian Arab Republic': 'Syria'
    }

    return c if not c in ct else ct[c]

def translate_plane(iata):
    # Some plane iata codes in routes.dat don't match any
    # in planes.dat. For these we'll use an approximation
    pl = {
        'DH8': 'DH1', # De Havilland Canada DHC-8-100 Dash 8 
        'CRJ': 'CR1', # Canadair Regional Jet 100
        'DC9': 'D91' # Douglas DC-9-10
    }
    return iata if not iata in pl else pl[iata]

def convert_if_exists(v, t):
        return t(v) if v else None




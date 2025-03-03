"""
Column mappings for BWARM tables based on specification
https://bwarm.ddex.net/bulk-communication-of-work-and-recording-metadata/6-bwarm-data-feed/
"""

COLUMN_MAPPING = {
    'parties': {
        'columns': [
            'party_record_id',
            'isni',
            'ipi_name_number',
            'cisac_society_id',
            'dpid',
            'full_name',
            'names_before_key_name',
            'key_name',
            'names_after_key_name',
            'contact_name',
            'email_address',
            'phone_number',
            'postal_address',
            'contact_information_may_not_be_valid'
        ],
        'array_columns': []
    },
    'releases': {
        'columns': [
            'release_record_id',
            'icpn',
            'release_title',
            'language_and_script_code',
            'release_subtitle',
            'display_artist_name',
            'display_artist_isni',
            'label_name',
            'distributor_name',
            'release_date',
            'original_data_provider_name',
            'original_data_provider_dpid',
            'is_data_provided_as_received',
        ],
        'array_columns': [
            'icpn',
            'label_name',
            'distributor_name',
            'release_date',
        ]
    },
    'releaseidentifiers': {
        'columns': [
            'proprietary_release_identifier_record_id',
            'release_record_id',
            'proprietary_id',
            'allocating_party_record_id',
        ],
        'array_columns': []
    },
    'unclaimedworkrightshares': {
        'columns': [
            'unclaimed_musical_work_right_share_record_id',
            'resource_record_id',
            'musical_work_record_id',
            'isrc',
            'dsp_resource_id',
            'resource_title',
            'resource_subtitle',
            'alternative_resource_title',
            'display_artist_name',
            'display_artist_isni',
            'duration',
            'unclaimed_right_share_percentage',
            'percentile_for_prioritisation',
        ],
        'array_columns': [
            'alternative_resource_title',
        ]
    },
    'recordings': {
        'columns': [
            'resource_record_id',
            'resource_type',
            'title',
            'display_artist_name',
            'isrc',
            'language_and_script_code',
            'sub_title',
            'display_artist_isni',
            'p_line',
            'c_line',
            'duration',
            'release_record_id',
            'studio_producer_name',
            'studio_producer_proprietary_id',
            'original_data_provider_name',
            'original_data_provider_dpid',
            'is_data_provided_as_received',
        ],
        'array_columns': [
            'p_line',
            'c_line',
            'studio_producer_name',
            'studio_producer_proprietary_id',
        ]
    },
    'recordingalternativetitles': {
        'columns': [
            'alternative_resource_title_record_id',
            'resource_record_id',
            'alternative_title',
            'language_and_script_code',
            'alternative_title_type',
        ],
        'array_columns': []
    },
    'recordingidentifiers': {
        'columns': [
            'proprietary_resource_identifier_record_id',
            'resource_record_id',
            'proprietary_id',
            'allocating_party_record_id',
        ],
        'array_columns': []
    },
    'workidentifiers': {
        'columns': [
            'proprietary_musical_work_identifier_record_id',
            'musical_work_record_id',
            'proprietary_id',
            'allocating_party_record_id',
        ],
        'array_columns': []
    },
    'workalternativetitles': {
        'columns': [
            'alternative_musical_work_title_record_id',
            'musical_work_record_id',
            'alternative_title',
            'language_and_script_code',
            'alternative_title_type',
        ],
        'array_columns': []
    },
    'workrightshares': {
        'columns': [
            'musical_work_right_share_record_id',
            'musical_work_record_id',
            'party_record_id',
            'party_role',
            'right_share_percentage',
            'right_share_type',
            'rights_type',
            'validity_start_date',
            'validity_end_date',
            'preceding_musical_work_right_share_record_id',
            'territory_code',
            'use_type',
        ],
        'array_columns': [
            'rights_type',
            'preceding_musical_work_right_share_record_id',
            'territory_code',
            'use_type',
        ]
    },
    'works': {
        'columns': [
            'musical_work_record_id',
            'iswc',
            'musical_work_title',
            'language_and_script_code',
            'opus_number',
            'composer_catalog_number',
            'nominal_duration',
            'has_right_share_in_dispute',
            'territory_of_public_domain',
            'is_arrangement_of_traditional_work',
            'alternative_musical_work_id_for_us_statutory_reversion',
            'us_statutory_reversion_date',
            'is_composite_musical_work'
        ],
        'array_columns': [
            'composer_catalog_number',
            'territory_of_public_domain'
        ]
    },
    'worksrecordings': {
        'columns': [
            'link_record_id',
            'musical_work_record_id',
            'resource_record_id'
        ],
        'array_columns': []
    },
}

def get_column_names(table_name):
    """Get the column names for a specific table based on BWARM spec"""
    table_config = COLUMN_MAPPING.get(table_name, {})
    return table_config.get('columns', [])

def get_array_columns(table_name):
    """Get the array column names for a specific table"""
    table_config = COLUMN_MAPPING.get(table_name, {})
    return table_config.get('array_columns', []) 
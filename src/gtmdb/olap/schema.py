"""ClickHouse DDL for the events table.

A single, wide, denormalized table that stores every GTM event with its
full enrichment context (graph dimensions materialized at write time).
This lets the agent issue arbitrary GROUP BY / WHERE queries in SQL without
needing to understand the graph topology.

Partition strategy:  toYYYYMM(occurred_at)
Sort key:            (tenant_id, event_category, event_type, occurred_at)
                     → covers the most common filter combinations.

All string-typed dimension columns default to '' so COUNT / GROUP BY
work predictably without NULL handling.
"""

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

EVENTS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {database}.events
(
    -- ------------------------------------------------------------------ --
    --  Primary identity                                                   --
    -- ------------------------------------------------------------------ --
    event_id        String,
    tenant_id       String,
    event_type      LowCardinality(String),
    event_category  LowCardinality(String),
    occurred_at     DateTime64(3, 'UTC'),
    source_node_id  String        DEFAULT '',
    source_label    LowCardinality(String) DEFAULT '',
    related_node_id String        DEFAULT '',
    related_label   LowCardinality(String) DEFAULT '',
    relation        LowCardinality(String) DEFAULT '',
    actor_id        String        DEFAULT '',

    -- ------------------------------------------------------------------ --
    --  Lead dimensions                                                    --
    -- ------------------------------------------------------------------ --
    lead_id         String        DEFAULT '',
    lead_status     LowCardinality(String) DEFAULT '',
    lead_source     LowCardinality(String) DEFAULT '',
    lead_company    String        DEFAULT '',
    lead_domain     String        DEFAULT '',
    lead_score      Float32       DEFAULT 0,
    lead_is_signup  UInt8         DEFAULT 0,
    lead_signup_date String       DEFAULT '',

    -- ------------------------------------------------------------------ --
    --  Contact dimensions                                                 --
    -- ------------------------------------------------------------------ --
    contact_id      String        DEFAULT '',
    contact_name    String        DEFAULT '',
    contact_title   String        DEFAULT '',
    contact_dept    String        DEFAULT '',
    contact_email   String        DEFAULT '',

    -- ------------------------------------------------------------------ --
    --  Account dimensions                                                 --
    -- ------------------------------------------------------------------ --
    account_id      String        DEFAULT '',
    account_name    String        DEFAULT '',
    account_domain  String        DEFAULT '',
    account_industry LowCardinality(String) DEFAULT '',
    account_type    LowCardinality(String) DEFAULT '',
    account_employees Int32       DEFAULT 0,
    account_arr     Float64       DEFAULT 0,

    -- ------------------------------------------------------------------ --
    --  Campaign dimensions (first-touch when multi-touch exists)         --
    -- ------------------------------------------------------------------ --
    campaign_id     String        DEFAULT '',
    campaign_name   String        DEFAULT '',
    campaign_channel LowCardinality(String) DEFAULT '',
    campaign_category LowCardinality(String) DEFAULT '',
    campaign_status LowCardinality(String) DEFAULT '',
    campaign_budget Float64       DEFAULT 0,

    -- ------------------------------------------------------------------ --
    --  Channel dimensions                                                 --
    -- ------------------------------------------------------------------ --
    channel_id      String        DEFAULT '',
    channel_name    String        DEFAULT '',
    channel_type    LowCardinality(String) DEFAULT '',

    -- ------------------------------------------------------------------ --
    --  Deal dimensions                                                    --
    -- ------------------------------------------------------------------ --
    deal_id         String        DEFAULT '',
    deal_name       String        DEFAULT '',
    deal_stage      LowCardinality(String) DEFAULT '',
    deal_amount     Float64       DEFAULT 0,
    deal_probability Float32      DEFAULT 0,
    deal_owner_id   String        DEFAULT '',
    deal_close_date String        DEFAULT '',

    -- ------------------------------------------------------------------ --
    --  Subscription event payload                                        --
    -- ------------------------------------------------------------------ --
    sub_event_type  LowCardinality(String) DEFAULT '',
    sub_plan_tier   LowCardinality(String) DEFAULT '',
    sub_plan_period LowCardinality(String) DEFAULT '',
    sub_arr         Float64       DEFAULT 0,
    sub_days_from_signup Int32    DEFAULT 0,
    sub_product_name String       DEFAULT '',

    -- ------------------------------------------------------------------ --
    --  Product & ProductAccount dimensions                               --
    -- ------------------------------------------------------------------ --
    product_id      String        DEFAULT '',
    product_name    String        DEFAULT '',
    product_type    LowCardinality(String) DEFAULT '',
    product_account_id     String DEFAULT '',
    product_account_name   String DEFAULT '',
    product_account_region LowCardinality(String) DEFAULT '',
    product_account_country LowCardinality(String) DEFAULT '',
    product_account_industry LowCardinality(String) DEFAULT '',
    product_account_size_group LowCardinality(String) DEFAULT '',
    product_account_is_paying UInt8 DEFAULT 0,

    -- ------------------------------------------------------------------ --
    --  Visitor dimensions                                                 --
    -- ------------------------------------------------------------------ --
    visitor_id       String       DEFAULT '',
    visitor_channel  LowCardinality(String) DEFAULT '',
    visitor_signup_flow LowCardinality(String) DEFAULT '',
    visitor_signup_cluster LowCardinality(String) DEFAULT '',
    visitor_dept     LowCardinality(String) DEFAULT '',
    visitor_seniority LowCardinality(String) DEFAULT '',
    visitor_product_intent LowCardinality(String) DEFAULT '',
    visitor_team_size LowCardinality(String) DEFAULT '',

    -- ------------------------------------------------------------------ --
    --  Content dimensions                                                 --
    -- ------------------------------------------------------------------ --
    content_id      String        DEFAULT '',
    content_name    String        DEFAULT '',
    content_type    LowCardinality(String) DEFAULT '',
    content_url     String        DEFAULT '',

    -- ------------------------------------------------------------------ --
    --  Flexible overflow (JSON for any extra properties)                 --
    -- ------------------------------------------------------------------ --
    extra           String        DEFAULT '{{}}'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (tenant_id, event_category, event_type, occurred_at)
SETTINGS index_granularity = 8192
"""

# Column list in ORDER that matches inserts — used by ClickHouseClient
EVENTS_COLUMNS: list[str] = [
    "event_id", "tenant_id", "event_type", "event_category", "occurred_at",
    "source_node_id", "source_label", "related_node_id", "related_label",
    "relation", "actor_id",
    # Lead
    "lead_id", "lead_status", "lead_source", "lead_company", "lead_domain",
    "lead_score", "lead_is_signup", "lead_signup_date",
    # Contact
    "contact_id", "contact_name", "contact_title", "contact_dept", "contact_email",
    # Account
    "account_id", "account_name", "account_domain", "account_industry",
    "account_type", "account_employees", "account_arr",
    # Campaign
    "campaign_id", "campaign_name", "campaign_channel", "campaign_category",
    "campaign_status", "campaign_budget",
    # Channel
    "channel_id", "channel_name", "channel_type",
    # Deal
    "deal_id", "deal_name", "deal_stage", "deal_amount", "deal_probability",
    "deal_owner_id", "deal_close_date",
    # Subscription
    "sub_event_type", "sub_plan_tier", "sub_plan_period", "sub_arr",
    "sub_days_from_signup", "sub_product_name",
    # Product
    "product_id", "product_name", "product_type",
    # ProductAccount
    "product_account_id", "product_account_name", "product_account_region",
    "product_account_country", "product_account_industry",
    "product_account_size_group", "product_account_is_paying",
    # Visitor
    "visitor_id", "visitor_channel", "visitor_signup_flow", "visitor_signup_cluster",
    "visitor_dept", "visitor_seniority", "visitor_product_intent", "visitor_team_size",
    # Content
    "content_id", "content_name", "content_type", "content_url",
    # Overflow
    "extra",
]

# BigQuery Best Practices Skill

## Overview

This skill provides guidance on using BigQuery effectively within GrowthNav, including query optimization, cost management, tenant isolation, and troubleshooting.

## When to Use This Skill

- Writing BigQuery queries for customer data
- Optimizing query performance and cost
- Understanding GrowthNav's tenant isolation patterns
- Troubleshooting query issues
- Estimating query costs before execution

## Tenant Isolation Pattern

GrowthNav uses **dataset-per-customer** isolation for data security and organization:

```
project: growthnav-prod
├── growthnav_topgolf/           # Topgolf's isolated data
│   ├── conversions
│   └── daily_metrics
├── growthnav_puttery/           # Puttery's isolated data
│   ├── conversions
│   └── daily_metrics
├── growthnav_medcorp_a/         # MedCorp A's data
│   └── ...
└── growthnav_registry/          # Shared customer registry
    └── customers
```

### Always Use TenantBigQueryClient

The TenantBigQueryClient automatically scopes queries to a customer's dataset:

```python
from growthnav.bigquery import TenantBigQueryClient

# Client automatically targets growthnav_topgolf dataset
client = TenantBigQueryClient(customer_id="topgolf")

# This query runs against growthnav_topgolf.conversions
result = client.query("SELECT * FROM conversions LIMIT 10")
```

### MCP Tool Usage

```
query_bigquery(
    customer_id="topgolf",
    sql="SELECT * FROM daily_metrics LIMIT 100"
)
```

The customer_id ensures isolation - queries cannot access other customers' data.

## Query Safety

### Automatic Validation

The `QueryValidator` blocks destructive operations:

| Operation | Status | Reason |
|-----------|--------|--------|
| SELECT | **Allowed** | Read-only |
| DROP | **Blocked** | Destructive |
| DELETE | **Blocked** | Data loss |
| TRUNCATE | **Blocked** | Data loss |
| INSERT | **Blocked*** | Requires `allow_writes=True` |
| UPDATE | **Blocked*** | Requires `allow_writes=True` |
| MERGE | **Blocked*** | Requires `allow_writes=True` |
| CREATE | **Blocked** | Schema modification |
| ALTER | **Blocked** | Schema modification |

*Write operations can be enabled for ETL pipelines.

### Automatic Warnings

You'll receive warnings for potentially expensive patterns:

```python
# Warning: SELECT * scans all columns
"SELECT * FROM conversions LIMIT 100"
# Suggestion: Specify columns to reduce data scanned

# Warning: Missing LIMIT clause
"SELECT id, value FROM conversions"
# Suggestion: Add LIMIT to prevent runaway queries
```

## Cost Optimization

### Always Estimate Before Running

```python
# Estimate cost before executing expensive query
estimate = client.estimate_cost("""
    SELECT *
    FROM conversions
    WHERE timestamp >= '2024-01-01'
""")

print(f"Will scan: {estimate['bytes_processed']:,} bytes")
print(f"Estimated cost: ${estimate['estimated_cost_usd']:.4f}")
print(f"Cached: {estimate['is_cached']}")
```

Via MCP:
```
estimate_query_cost(
    customer_id="topgolf",
    sql="SELECT * FROM large_table"
)
```

### BigQuery Pricing

| Tier | Price | Notes |
|------|-------|-------|
| On-demand | $6.25/TB | Per query, first 1TB/month free |
| Flat-rate | $2,000/mo | 100 slots, unlimited queries |

### Cost-Saving Patterns

#### 1. Use Column Projection

```sql
-- Good: Only select needed columns ($0.10)
SELECT customer_id, value, timestamp
FROM conversions
WHERE date(timestamp) = '2025-01-15'

-- Bad: Scans all columns ($1.50)
SELECT *
FROM conversions
WHERE date(timestamp) = '2025-01-15'
```

#### 2. Leverage Partitioning

Tables are partitioned by date. Use partition filters:

```sql
-- Good: Prunes partitions, scans only 1 day
SELECT * FROM conversions
WHERE DATE(timestamp) = '2025-01-15'

-- Bad: Scans ALL partitions (expensive!)
SELECT * FROM conversions
WHERE EXTRACT(MONTH FROM timestamp) = 1
```

#### 3. Use LIMIT During Development

```sql
-- Good: Limited during exploration
SELECT * FROM conversions LIMIT 100

-- Bad: Returns millions of rows
SELECT * FROM conversions
```

#### 4. Avoid Repeated Queries

Use caching (24-hour cache) and materialized views:

```python
result = client.query(sql)
print(f"Cache hit: {result.cache_hit}")  # True = no additional cost
```

## Query Patterns

### Basic Metrics Query

```sql
SELECT
    DATE(timestamp) as date,
    conversion_type,
    COUNT(*) as count,
    SUM(value) as total_value,
    AVG(value) as avg_value
FROM conversions
WHERE DATE(timestamp) BETWEEN '2025-01-01' AND '2025-01-31'
GROUP BY 1, 2
ORDER BY 1, 2
LIMIT 1000
```

### Parameterized Queries (Safe)

```python
# Safe: Uses parameterized queries (no SQL injection)
result = client.query(
    "SELECT * FROM conversions WHERE value > @min_value AND source = @source",
    params={"min_value": 100.0, "source": "pos"}
)
```

Via MCP:
```
query_bigquery(
    customer_id="topgolf",
    sql="SELECT * FROM conversions WHERE value > @min_value",
    params={"min_value": 100.0}
)
```

### Cross-Customer Industry Analysis

```python
from growthnav.bigquery import CustomerRegistry, Industry

registry = CustomerRegistry()
golf_customers = registry.get_customers_by_industry(Industry.GOLF)

# Aggregate metrics across industry
industry_metrics = []
for customer in golf_customers:
    client = TenantBigQueryClient(customer_id=customer.customer_id)
    result = client.query("""
        SELECT
            SUM(value) as total_revenue,
            COUNT(*) as conversion_count
        FROM conversions
        WHERE DATE(timestamp) >= '2025-01-01'
    """)
    industry_metrics.append({
        "customer": customer.customer_id,
        **result.rows[0]
    })
```

### Async Queries for Large Results

```python
import asyncio

async def get_large_dataset():
    client = TenantBigQueryClient(customer_id="topgolf")
    result = await client.query_async(
        "SELECT * FROM daily_metrics",
        max_results=100000
    )
    return result

# Run async
data = asyncio.run(get_large_dataset())
```

## Schema Information

### Get Table Schema

```python
schema = client.get_table_schema("conversions")
for field in schema:
    print(f"{field['name']}: {field['type']} ({field['mode']})")
```

Via MCP:
```
get_table_schema(
    customer_id="topgolf",
    table_name="conversions"
)
```

### Standard Table Schemas

#### conversions

| Column | Type | Mode | Description |
|--------|------|------|-------------|
| conversion_id | STRING | REQUIRED | Unique ID |
| customer_id | STRING | REQUIRED | Customer identifier |
| transaction_id | STRING | NULLABLE | External transaction ID |
| conversion_type | STRING | REQUIRED | purchase, lead, signup |
| source | STRING | REQUIRED | pos, crm, loyalty |
| value | FLOAT64 | REQUIRED | Monetary value |
| currency | STRING | REQUIRED | USD, EUR, etc. |
| timestamp | TIMESTAMP | REQUIRED | When conversion occurred |
| gclid | STRING | NULLABLE | Google Click ID |
| fbclid | STRING | NULLABLE | Facebook Click ID |
| utm_source | STRING | NULLABLE | UTM source |
| attributed_platform | STRING | NULLABLE | google_ads, meta, etc. |

#### daily_metrics

| Column | Type | Mode | Description |
|--------|------|------|-------------|
| date | DATE | REQUIRED | Metric date |
| customer_id | STRING | REQUIRED | Customer identifier |
| platform | STRING | REQUIRED | google_ads, meta, etc. |
| campaign_id | STRING | NULLABLE | Campaign identifier |
| impressions | INT64 | NULLABLE | Ad impressions |
| clicks | INT64 | NULLABLE | Ad clicks |
| spend | FLOAT64 | NULLABLE | Ad spend |
| conversions | INT64 | NULLABLE | Conversion count |
| revenue | FLOAT64 | NULLABLE | Revenue generated |

## Troubleshooting

### "Access Denied" Errors

1. **Verify customer_id is correct**
   ```python
   customer = registry.get_customer("topgolf")
   print(customer.dataset)  # Should match expected dataset
   ```

2. **Check service account has access**
   - Service account needs BigQuery Data Viewer role
   - Dataset must exist

3. **Confirm dataset exists**
   ```python
   provisioner = DatasetProvisioner()
   exists = provisioner.dataset_exists("topgolf")
   ```

### Slow Queries

1. **Check partition pruning**
   - Use `DATE(timestamp) = '...'` not `EXTRACT(...)`
   - Add partition filter to WHERE clause

2. **Reduce columns**
   - Replace `SELECT *` with specific columns

3. **Add appropriate filters**
   - Filter early in the query

4. **Check query plan**
   ```sql
   -- Add EXPLAIN to see query plan
   EXPLAIN SELECT * FROM conversions WHERE ...
   ```

### High Costs

1. **Always use estimate_cost() first**
2. **Use LIMIT during development**
3. **Prefer column projection**
4. **Check for cache hits**
5. **Filter on partitioned columns**

### Query Blocked

```
ValueError: DROP statements are not allowed
```

This is intentional safety. If you need write access:
```python
# Only for ETL pipelines, not interactive use
validator = QueryValidator()
result = validator.validate(sql, allow_writes=True)
```

## Performance Tips

1. **Clustering**: Tables are clustered by common filter columns
2. **Partitioning**: Time-based tables partition by date
3. **Caching**: Queries cache for 24 hours (check `cache_hit`)
4. **Slots**: On-demand pricing, no slot reservations needed

## Related Skills

- **customer-onboarding**: Set up new customer datasets
- **analytics-reporting**: Generate reports from query results

## Related MCP Tools

- `query_bigquery`: Execute queries
- `estimate_query_cost`: Estimate costs before execution
- `get_table_schema`: Get table column information
- `get_customer`: Get customer configuration
- `list_customers_by_industry`: Find customers for benchmarking

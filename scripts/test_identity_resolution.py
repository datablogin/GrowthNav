#!/usr/bin/env python3
"""Test identity resolution on Nothing Bundt Cakes Snowflake data.

This script:
1. Connects to Snowflake MARKETING_DB.MART_SALE.SUMMARY_ORDERS
2. Fetches sample data with email and CARDFINGERPRINT
3. Tests identity resolution to find overlapping identities
"""

import os
from datetime import datetime, timedelta

import snowflake.connector


def get_connection():
    """Create Snowflake connection."""
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account="NOTHINGBUNDTCAKES-NOTHINGBUNDTCAKES",
        warehouse="MARKETING_WH",
        database="MARKETING_DB",
        schema="MART_SALE",
        role="MARKETING_ROLE",
    )


def explore_schema():
    """Explore the SUMMARY_ORDERS table schema."""
    print("=" * 60)
    print("Exploring SUMMARY_ORDERS schema")
    print("=" * 60)

    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("DESCRIBE TABLE SUMMARY_ORDERS")
        columns = cursor.fetchall()

        print(f"\nFound {len(columns)} columns:\n")
        for col in columns:
            print(f"  {col[0]:40} {col[1]}")

        # Check for identity-related columns
        print("\n" + "=" * 60)
        print("Identity-related columns:")
        print("=" * 60)

        identity_keywords = ['email', 'phone', 'card', 'finger', 'customer', 'user', 'member', 'loyalty']
        for col in columns:
            col_name = col[0].lower()
            if any(kw in col_name for kw in identity_keywords):
                print(f"  {col[0]:40} {col[1]}")

    finally:
        cursor.close()
        conn.close()


def sample_identity_data():
    """Sample data with identity fields."""
    print("\n" + "=" * 60)
    print("Sampling identity data")
    print("=" * 60)

    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Get sample of records with both email and card fingerprint
        query = """
        SELECT
            ORDER_GUID,
            EMAIL,
            CARDFINGERPRINT,
            EMAIL_BY_FINGERPRINT,
            ORDER_BUSINESS_DATE,
            NET_SALES
        FROM SUMMARY_ORDERS
        WHERE EMAIL IS NOT NULL
          AND CARDFINGERPRINT IS NOT NULL
        LIMIT 20
        """

        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        print(f"\nSample of {len(rows)} records with both EMAIL and CARDFINGERPRINT:\n")
        for row in rows[:5]:
            record = dict(zip(columns, row))
            print(f"  Order: {record['ORDER_GUID']}")
            print(f"    Email: {record['EMAIL']}")
            print(f"    Card Fingerprint: {record['CARDFINGERPRINT']}")
            print(f"    Email by Fingerprint: {record['EMAIL_BY_FINGERPRINT']}")
            print()

    finally:
        cursor.close()
        conn.close()


def analyze_identity_overlap():
    """Analyze overlap between emails and card fingerprints."""
    print("\n" + "=" * 60)
    print("Analyzing identity overlap")
    print("=" * 60)

    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Find cases where same fingerprint maps to multiple emails
        query = """
        SELECT
            CARDFINGERPRINT,
            COUNT(DISTINCT EMAIL) as email_count,
            LISTAGG(DISTINCT EMAIL, ', ') WITHIN GROUP (ORDER BY EMAIL) as emails
        FROM SUMMARY_ORDERS
        WHERE CARDFINGERPRINT IS NOT NULL
          AND EMAIL IS NOT NULL
        GROUP BY CARDFINGERPRINT
        HAVING COUNT(DISTINCT EMAIL) > 1
        ORDER BY email_count DESC
        LIMIT 20
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        print(f"\nFound {len(rows)} card fingerprints with multiple emails:\n")
        for row in rows[:10]:
            fingerprint, email_count, emails = row
            print(f"  Fingerprint: {fingerprint}")
            print(f"    Email count: {email_count}")
            print(f"    Emails: {str(emails)[:100]}...")
            print()

        # Find cases where same email maps to multiple fingerprints
        query2 = """
        SELECT
            EMAIL,
            COUNT(DISTINCT CARDFINGERPRINT) as fingerprint_count,
            LISTAGG(DISTINCT CARDFINGERPRINT, ', ') WITHIN GROUP (ORDER BY CARDFINGERPRINT) as fingerprints
        FROM SUMMARY_ORDERS
        WHERE CARDFINGERPRINT IS NOT NULL
          AND EMAIL IS NOT NULL
        GROUP BY EMAIL
        HAVING COUNT(DISTINCT CARDFINGERPRINT) > 1
        ORDER BY fingerprint_count DESC
        LIMIT 20
        """

        cursor.execute(query2)
        rows2 = cursor.fetchall()

        print(f"\nFound {len(rows2)} emails with multiple card fingerprints:\n")
        for row in rows2[:10]:
            email, fp_count, fingerprints = row
            print(f"  Email: {email}")
            print(f"    Fingerprint count: {fp_count}")
            print(f"    Fingerprints: {str(fingerprints)[:80]}...")
            print()

        # Summary stats
        query3 = """
        SELECT
            COUNT(*) as total_orders,
            COUNT(DISTINCT EMAIL) as unique_emails,
            COUNT(DISTINCT CARDFINGERPRINT) as unique_fingerprints,
            COUNT(CASE WHEN EMAIL IS NOT NULL AND CARDFINGERPRINT IS NOT NULL THEN 1 END) as orders_with_both
        FROM SUMMARY_ORDERS
        """

        cursor.execute(query3)
        stats = cursor.fetchone()

        print("\n" + "=" * 60)
        print("Summary Statistics")
        print("=" * 60)
        print(f"  Total orders: {stats[0]:,}")
        print(f"  Unique emails: {stats[1]:,}")
        print(f"  Unique card fingerprints: {stats[2]:,}")
        print(f"  Orders with both email + fingerprint: {stats[3]:,}")

    finally:
        cursor.close()
        conn.close()


def test_identity_linker():
    """Test our IdentityLinker with real Snowflake data."""
    print("\n" + "=" * 60)
    print("Testing IdentityLinker with Snowflake data")
    print("=" * 60)

    from growthnav.connectors.identity import IdentityLinker, IdentityType

    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Fetch sample records with identity fields
        query = """
        SELECT
            ORDER_GUID as id,
            EMAIL as email,
            CARDFINGERPRINT as hashed_cc,
            ORDER_BUSINESS_DATE,
            NET_SALES
        FROM SUMMARY_ORDERS
        WHERE EMAIL IS NOT NULL
          AND CARDFINGERPRINT IS NOT NULL
        LIMIT 1000
        """

        cursor.execute(query)
        columns = [desc[0].lower() for desc in cursor.description]
        rows = cursor.fetchall()

        # Convert to list of dicts
        records = [dict(zip(columns, row)) for row in rows]
        print(f"\nFetched {len(records)} records from Snowflake")

        # Test identity resolution
        linker = IdentityLinker()
        linker.add_records(records, source="snowflake_orders")

        print(f"Added {len(linker._records)} normalized records")

        # Run deterministic resolution
        identities = linker.resolve_deterministic()

        print(f"\nResolved to {len(identities)} unique identities")

        # Analyze results
        single_fragment = sum(1 for i in identities if len(i.fragments) == 1)
        multi_fragment = sum(1 for i in identities if len(i.fragments) > 1)

        print(f"  Single-fragment identities: {single_fragment}")
        print(f"  Multi-fragment identities: {multi_fragment}")

        # Show some multi-fragment identities
        print("\nSample multi-fragment identities:")
        for identity in identities[:5]:
            if len(identity.fragments) > 1:
                print(f"\n  Global ID: {identity.global_id[:8]}...")
                print(f"  Fragments: {len(identity.fragments)}")
                for frag in identity.fragments[:3]:
                    print(f"    {frag.fragment_type.value}: {str(frag.fragment_value)[:30]}...")

    finally:
        cursor.close()
        conn.close()


def test_cross_record_linking():
    """Test linking across records with shared fingerprints."""
    print("\n" + "=" * 60)
    print("Testing cross-record identity linking")
    print("=" * 60)

    from growthnav.connectors.identity import IdentityLinker, IdentityType

    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Find a fingerprint that has multiple emails, then fetch those records
        query = """
        WITH fingerprints_with_multiple_emails AS (
            SELECT CARDFINGERPRINT
            FROM SUMMARY_ORDERS
            WHERE CARDFINGERPRINT IS NOT NULL
              AND EMAIL IS NOT NULL
            GROUP BY CARDFINGERPRINT
            HAVING COUNT(DISTINCT EMAIL) > 1
            LIMIT 10
        )
        SELECT
            ORDER_GUID as id,
            EMAIL as email,
            CARDFINGERPRINT as hashed_cc,
            NET_SALES
        FROM SUMMARY_ORDERS
        WHERE CARDFINGERPRINT IN (SELECT CARDFINGERPRINT FROM fingerprints_with_multiple_emails)
          AND EMAIL IS NOT NULL
        LIMIT 500
        """

        cursor.execute(query)
        columns = [desc[0].lower() for desc in cursor.description]
        rows = cursor.fetchall()

        # Convert to list of dicts
        records = [dict(zip(columns, row)) for row in rows]
        print(f"\nFetched {len(records)} records with overlapping fingerprints")

        # Show some raw data
        print("\nSample records showing overlapping fingerprints:")
        from collections import defaultdict
        fingerprint_to_emails = defaultdict(set)
        for r in records:
            fingerprint_to_emails[r['hashed_cc']].add(r['email'])

        for fp, emails in list(fingerprint_to_emails.items())[:5]:
            if len(emails) > 1:
                print(f"\n  Fingerprint: {fp}")
                print(f"    Emails: {list(emails)[:5]}")

        # Test identity resolution
        linker = IdentityLinker()
        linker.add_records(records, source="snowflake_orders")

        print(f"\nAdded {len(linker._records)} normalized records")

        # Run deterministic resolution
        identities = linker.resolve_deterministic()

        print(f"\nResolved to {len(identities)} unique identities")

        # Count identities by fragment count
        fragment_counts = defaultdict(int)
        for identity in identities:
            fragment_counts[len(identity.fragments)] += 1

        print("\nIdentities by fragment count:")
        for count, num in sorted(fragment_counts.items()):
            print(f"  {count} fragments: {num} identities")

        # Show identities with multiple emails (successful linking!)
        print("\nIdentities with multiple emails (cross-record links):")
        multi_email_identities = [i for i in identities if len(i.emails) > 1]
        print(f"Found {len(multi_email_identities)} identities with multiple emails")

        for identity in multi_email_identities[:5]:
            print(f"\n  Global ID: {identity.global_id[:8]}...")
            print(f"  Match probability: {identity.match_probability}")
            print(f"  Emails ({len(identity.emails)}): {identity.emails[:3]}")
            hashed_ccs = [f.fragment_value for f in identity.fragments
                         if f.fragment_type == IdentityType.HASHED_CC]
            print(f"  Card fingerprints ({len(hashed_ccs)}): {hashed_ccs[:3]}")

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    print("Nothing Bundt Cakes - Identity Resolution Test")
    print("=" * 60)

    # Step 1: Explore schema
    explore_schema()

    # Step 2: Sample data
    sample_identity_data()

    # Step 3: Analyze overlap
    analyze_identity_overlap()

    # Step 4: Test identity linker
    test_identity_linker()

    # Step 5: Test cross-record linking
    test_cross_record_linking()

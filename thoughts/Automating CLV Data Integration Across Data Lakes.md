# **Automated Semantic Unification and Identity Resolution for Scalable Customer Lifetime Value Analytics in Heterogeneous Data Lakes**

## **1\. Executive Overview: The Semantic Scalability Challenge in Multi-Tenant Analytics**

The contemporary data landscape has shifted fundamentally from monolithic, single-tenant architectures to complex, multi-tenant ecosystems. This transition is particularly acute for holding companies, private equity firms, and consultancy aggregators tasked with analyzing performance across a portfolio of distinct operating entities. The user’s objective—to rapidly onboard distinct companies with heterogeneous data lakes into a unified Customer Lifetime Value (CLV) framework—represents one of the frontier challenges in modern data engineering: **Automated Semantic Interoperability**.

In the traditional paradigm of data warehousing, onboarding a new tenant—whether a retailer, a franchise, or a portfolio company—imposes a severe "ETL tax." Data engineers are required to manually inspect source systems (CRM, POS, ERP), decipher cryptic column names, and map them to a rigid target schema. This process is inherently unscalable. It is labor-intensive, prone to human error, and resistant to the velocity required by modern agile analytics. The problem is compounded when the analytical goal is as complex as CLV, which necessitates precise, harmonized inputs regarding customer identity, transaction timing, monetary value, and acquisition channels.1

The core friction in this scenario is not merely technical but semantic. As the user notes, "one company thinks of what a consumer buys at the store as an order, another thinks of it as a ticket." This is a conceptual divergence that requires an intelligent system to recognize that ticket and order are semantically equivalent in the context of a transaction, despite potentially distinct attribute compositions. Furthermore, the divergence in identity resolution mechanisms—where one entity tracks users by hashed credit card numbers and another by email addresses—creates a disjointed view of the customer that traditional deterministic joining cannot resolve.

Recent research suggests that the solution lies in a paradigm shift from **Schema-on-Write** (enforcing standards at the source) to **AI-Driven Schema-on-Read** (interpreting semantics at ingestion). By deploying **Intelligent Semantic Discovery** models utilizing deep learning (such as Sherlock and Sato) and **Probabilistic Identity Resolution** frameworks (such as Splink and Zingg), it is possible to construct a "Composable Customer Data Platform (CDP)" that dynamically adapts to incoming data structures.3

This report provides an exhaustive, expert-level analysis of the methodologies, architectures, and algorithms required to build such a system. It explores the theoretical underpinnings of data heterogeneity, the mechanics of deep learning for type detection, the mathematics of probabilistic record linkage, and the integration of "walled garden" advertising data from Google and Meta. The ultimate goal is to define a robust, automated pipeline that creates the "Golden Record" necessary for predictive CLV and marketing attribution, minimizing the manual "picking through" of data that currently bottlenecks the onboarding process.

## ---

**2\. Theoretical Framework: From Structural to Semantic Heterogeneity**

To automate the ingestion of diverse data lakes, one must first rigorously classify the types of heterogeneity that exist between them. The literature distinguishes between three primary levels of data conflict that an automated system must resolve without human intervention. Understanding these distinctions is critical because different algorithmic approaches are required to solve each level of discord.5

### **2.1 The Taxonomy of Data Heterogeneity**

When onboarding a new company's data lake, the system encounters friction at the structural, semantic, and instance levels.

#### **2.1.1 Structural and Schema Heterogeneity**

Structural heterogeneity refers to differences in the logical arrangement of data. This includes variations in table names, column names, and data types. For example, Company A may store transaction data in a table named orders with a column total\_amt formatted as a floating-point number. Company B might use a table named trans\_log with a column final\_price formatted as a string with currency symbols.

While this form of heterogeneity is the most visible, it is often the easiest to resolve using traditional schema matching techniques. However, in the context of the user's query, structural heterogeneity is merely the surface layer of a deeper problem. The "data lakes" mentioned likely contain unstructured or semi-structured data (JSON, Parquet, Avro), where the schema itself may evolve over time.7

#### **2.1.2 Semantic Heterogeneity**

The user’s query specifically highlights the challenge of **Semantic Heterogeneity**: "One company thinks of what a consumer buys at the store as an order, another thinks of it as a ticket." This is a divergence in *meaning* and *interpretation*. Semantic heterogeneity occurs when different concepts are used to describe the same real-world entity, or conversely, when the same term is used to describe different entities.5

In a retail context, this often manifests in subtle but critical ways:

* **Concept Divergence:** A "Ticket" in a POS system might represent a single session of scanning items, which could include multiple split payments. An "Order" in an online system (OLO) typically represents a single payment and shipment. Mapping these to a common "Transaction" entity requires reconciling these conceptual differences.  
* **Attribute Ambiguity:** A column named date in one system might refer to the *transaction date*, while in another, it refers to the *settlement date* or *shipping date*. An automated system must discern the semantic intent of the column to ensure that the Recency ($t\_x$) calculation in the CLV model is accurate.

#### **2.1.3 Instance Heterogeneity**

Instance heterogeneity refers to differences in how the same real-world entity—specifically the customer—is represented across different data sources. This is the core of the user's identity resolution problem: "One company tracks users by their credit card number (hashed for privacy) the other uses email address."

This level of heterogeneity requires **Identity Resolution (IdRes)**. The system must determine if the entity represented by Hash\_CC: 8x9s8d... in the POS system is the same real-world person as Email: jane.doe@example.com in the OLO system. This is complicated by the lack of a shared foreign key. Traditional database joins fail here; the solution requires probabilistic modeling and graph theory to infer relationships based on shared attributes or behavioral patterns.9

### **2.2 The Limitations of Traditional Approaches**

Historically, organizations addressed these heterogeneities through manual "picking" and coding. ETL developers would write custom scripts for each new data source, explicitly mapping source.T\_VAL to target.Amount. This approach relies on **Metadata-Based Matching**, utilizing column names and schema definitions to guide the integration.

However, research shows that metadata is often unreliable. Column names can be obfuscated (e.g., legacy ERP systems using COL\_001, VAR\_X) or ambiguous. Regular Expressions (Regex) and string distance metrics (like Levenshtein distance) can catch obvious matches (e.g., matching e-mail to email), but they fail to capture semantic context. They cannot determine that a column named ticket\_val contains monetary data while ticket\_id contains ordinal data, solely based on the text string.

To solve the user's problem of "analyzing the data and its dimensionality" automatically, we must move to **Content-Based Profiling** using machine learning.

## ---

**3\. Automated Semantic Discovery: The "Sherlock" and "Sato" Approach**

The "holy grail" of automated onboarding is **Semantic Type Detection**. The most robust method for determining "what columns we need to include" is to analyze the **data distribution** and **content** rather than just the metadata. Recent academic literature has produced powerful deep learning models designed specifically for this task.

### **3.1 Sherlock: Deep Learning for Semantic Typing**

Developed by researchers at MIT and the creators of the VizNet corpus, **Sherlock** is a deep learning architecture designed to detect semantic types (e.g., "Industry", "Country", "Product Name", "Sales Value") by analyzing the raw values within a column. It treats column classification as a multi-class classification problem, processing 78 predefined semantic types with a support-weighted F1 score of 0.89.4

Sherlock operates by extracting four distinct feature sets from the raw data of a column:

| Feature Category | Description | Relevance to CLV |
| :---- | :---- | :---- |
| **Global Statistics** | Extracts mean, variance, min, max, entropy, and sparsity. | Distinguishes Transaction\_Amount (continuous, positive) from Customer\_ID (uniform distribution, high entropy). |
| **Character Distributions** | Analyzes the frequency of 96 ASCII characters (digits, letters, symbols). | Distinguishes Email (high frequency of @ and .) from Phone (digits and \- or ()). |
| **Word Embeddings** | Uses pre-trained embeddings (e.g., GloVe) to interpret text values. | Identifies Product\_Category (e.g., "Shoes," "Shirt") based on semantic similarity of terms. |
| **Paragraph Vectors** | Aggregates the semantic meaning of the entire column into a single vector. | Provides a holistic view of the column's content "topic." |

By feeding a new company's data into a pre-trained Sherlock model, the system can probabilistically assert: "Column X7 contains Credit Card Numbers" or "Column P\_Val contains Monetary Transaction Amounts," regardless of what the column is named. This directly addresses the user's need to avoid "picking through" data manually.

### **3.2 Sato: Adding Contextual Awareness**

While Sherlock analyzes columns in isolation, **Sato (Semantics-Aware Table Operations)** incorporates **contextual awareness**. It recognizes that columns in a relational database do not exist in a vacuum; they interact with one another. The semantic type of one column often dictates the probability of another.13

Sato improves upon Sherlock by adding a **Topic Modeling** layer and a **Structured Prediction** module (often using Conditional Random Fields or CRFs).

* **The Context Hypothesis:** If a table contains a column identified with high confidence as "Product Name" and another as "Unit Price," the probability that a third, ambiguous numeric column is "Quantity" increases significantly. Conversely, if the table contains "City" and "Zip Code," the ambiguous column is likely "State" rather than "Quantity."  
* **Implementation:** Sato combines the single-column feature vectors from Sherlock with a "table topic" vector. It then optimizes the global assignment of types across the entire table to maximize coherence.

Application to the User’s Problem:  
For the user's multi-tenant scenario, the recommended architecture involves running a "profiling job" on every new data lake. This job samples 1,000 rows from every table and passes them through a Sato-style inference engine. The output is a Semantic Map that tags columns with standardized concepts required for CLV: Customer\_Identifier, Transaction\_Timestamp, Transaction\_Value, Product\_ID.

### **3.3 Large Language Models (LLMs) and Zero-Shot Schema Matching**

While Sato and Sherlock are specialized models, the emergence of Large Language Models (LLMs) in 2024 and 2025 has provided a powerful, generalized alternative. LLMs possess broad semantic knowledge and can perform **Zero-Shot Schema Matching**, mapping source columns to a target schema without specific training on the user's data.16

#### **3.3.1 RAG-Based Schema Mapping**

For data lakes containing thousands of tables, passing every table to an LLM context window is cost-prohibitive and slow. A **Retrieval-Augmented Generation (RAG)** approach is preferred.

1. **Metadata Embedding:** The schema metadata (table names, column names, descriptions if available) of the source system is embedded into a vector database (e.g., Pinecone, Milvus).18  
2. **Semantic Querying:** The system queries the vector database for concepts related to the target CLV model. For example, it queries for "Revenue," "Cost," "Customer," and "Date."  
3. **LLM Verification:** The RAG system retrieves the top candidate tables and columns. An LLM (like GPT-4 or Claude 3.5) is then prompted with a sample of data from these candidates to verify the match.  
   * *Prompt Example:* "Analyze the following 10 rows from column T\_VAL. Does this data represent a transaction amount? If so, extract the currency unit."

#### **3.3.2 Schema-Aware Prompting**

Research demonstrates that "schema-aware prompting" significantly outperforms generic prompting. The prompt must explicitly constrain the LLM to map to the target dimensionality of the Common Information Model (CIM).

* *Technique:* Provide the LLM with the DDL (Data Definition Language) of the *Target* table (the "Golden Record" schema) and the DDL of the *Source* table. Ask the LLM to generate a SQL SELECT statement or a **dbt** YAML file that maps Source to Target.  
* *Benefit:* This automates the generation of the transformation code, which is the most time-consuming part of onboarding a new tenant.19

### **3.4 Evaluation and Benchmarking: The Valentine Framework**

To ensure that these automated mappings are reliable, the system must include an evaluation loop. **Valentine** is an open-source framework designed to benchmark schema matching techniques. It provides a standardized way to score the accuracy of different matching algorithms (Coma, Cupid, EmbDI) against known ground truths.21

In a production environment, the Valentine framework can be adapted for **Continuous Evaluation**. When a new company is onboarded, the system's automated mappings should be scored against a "golden set" of previous successful mappings or verified by a human expert in a "Human-in-the-Loop" (HITL) workflow. If the system's confidence score for mapping user\_rank to customer\_segment is low, it flags the mapping for review, ensuring high data quality for the CLV models.23

## ---

**4\. The Target Dimensionality: A Common Information Model (CIM) for CLV**

The user asks: *"What columns we need to include?"* To answer this, we must look beyond general retail data models and focus specifically on the mathematical requirements of **Customer Lifetime Value (CLV)** calculations.

The most robust statistical models for CLV in non-contractual settings (like retail) are the **Buy Till You Die (BTYD)** models, specifically the **BG/NBD** (Beta-Geometric/Negative Binomial Distribution) model and the **Pareto/NBD** model. These models do not require deep demographic data; they rely on **RFM (Recency, Frequency, Monetary)** vectors. Therefore, the target dimensionality must be optimized to produce these vectors.25

### **4.1 The Core Entities of the CIM**

Based on the Microsoft Common Data Model for Retail and Snowflake's Retail Data Model, the following entities form the necessary "Golden Record" schema.27

#### **4.1.1 The Unified Customer Entity (Dimension)**

This entity represents the resolved identity of the consumer.

* **Mandatory Attributes:**  
  * Global\_Customer\_ID: A unique UUID generated by the Identity Resolution system.  
  * Source\_System\_IDs: An array or map containing the original IDs (e.g., { "pos": "123", "email": "jane@doe.com" }).  
* **CLV-Critical Attributes:**  
  * Acquisition\_Date: The date of the first observed transaction. This defines the start of the customer's lifespan ($T$).  
  * Acquisition\_Channel: The source attributed to the first interaction (e.g., Google Ads, Organic, Referral). This is vital for calculating **CAC (Customer Acquisition Cost)** to derive profitability (LTV:CAC ratio).  
  * Cohort\_Month: Derived from Acquisition Date, used for cohort retention analysis.

#### **4.1.2 The Unified Transaction Entity (Fact)**

This is the engine of the CLV calculation. It aggregates data from POS, OLO, and ERP.

* **Mandatory Attributes:**  
  * Transaction\_ID: Unique key.  
  * Global\_Customer\_ID: Foreign Key to the Customer entity.  
  * Transaction\_Timestamp: The precise time of the purchase. This is critical for calculating **Recency ($t\_x$)**, which is the time between the first and last transaction.  
  * Gross\_Amount: The total value paid.  
  * Net\_Amount: The value minus tax and discounts. *Note:* CLV should generally be calculated on Net Revenue or Gross Margin, not Gross Revenue.  
  * Currency\_Code: Essential for multi-national portfolios.  
* **Nuance \- Returns and Cancellations:**  
  * Transaction\_Type: A categorical field (Sale, Return, Exchange).  
  * Is\_Return: A boolean flag.  
  * *Insight:* One of the most common failures in CLV modeling is ignoring returns. A customer who buys $1000 of goods and returns $900 has a vastly different CLV than one who keeps $100. The Semantic Profiler (Sato) must be tuned to detect "negative" transactions or "return" flags in the source data and map them correctly to this field.2

#### **4.1.3 The Product Entity (Dimension)**

While BTYD models don't strictly require product data, understanding *what* drives value is crucial for actionable insights.

* **Mandatory Attributes:**  
  * Product\_ID / SKU  
  * Product\_Category: Hierarchy levels (e.g., Apparel \-\> Shoes \-\> Running).  
  * Unit\_Cost: Essential for calculating **Profitability CLV**. If the user wants to know the *profit* lifetime value, the system must ingest cost data from the ERP/Oracle system.

#### **4.1.4 The Touchpoint Entity (Fact)**

To relate ad exposure to value, as requested ("relate whether or not the customer saw advertising").

* **Mandatory Attributes:**  
  * Touchpoint\_ID  
  * Global\_Customer\_ID: Probabilistic link.  
  * Platform: Google, Meta, TikTok, Email.  
  * Campaign\_ID: Foreign key to ad spend data.  
  * Interaction\_Type: Impression, Click, Conversion.  
  * Timestamp: Critical for attribution windows.

### **4.2 Analytical Dimensionality: RFM Summary Statistics**

To feed the predictive models, the raw transaction data must be transformed into a specific vector format known as the **RFM Summary Statistics**.26 The automated pipeline must produce a derived table with the following dimensions for each customer:

| Parameter | Definition | Impact on CLV Model |
| :---- | :---- | :---- |
| **Frequency ($x$)** | The number of repeat purchases (total purchases minus one) in the observation period. | Drivers the "transaction rate" ($\\lambda$) in the BG/NBD model. |
| **Recency ($t\_x$)** | The duration between the first and the last transaction. | Determines the probability that the customer is still "alive" (active). If $t\_x$ is close to $T$, the customer is likely active. |
| **Time ($T$)** | The duration between the first transaction and the end of the observation period. | Defines the opportunity window for transactions. |
| **Monetary Value ($m$)** | The average value of the transactions. | Used in the Gamma-Gamma submodel to predict the value of future transactions. |

**Insight:** The semantic detection engine must be extremely precise with *timestamps*. An error in mapping the Transaction\_Timestamp—for example, mapping a Ship\_Date that is populated days after the order—can distort the Recency calculation, leading to inaccurate churn predictions.

## ---

**5\. Identity Resolution: Unifying the Customer Across Walled Gardens**

The user’s query identifies a critical point of friction: "One company tracks users by their credit card number (hashed for privacy) the other uses email address." This is the **Identity Resolution** challenge. Furthermore, integrating marketing data from Google and Meta requires matching these internal identifiers to external, walled-garden identities.

### **5.1 Probabilistic Record Linkage**

Since there is no shared "Foreign Key" across these companies and systems, the system must use **Probabilistic Record Linkage** (also known as Entity Resolution or Deduplication). This involves comparing records to calculate a "match score" based on the similarity of their attributes.29

#### **5.1.1 Splink: Scalable, Unsupervised Linkage**

The research highly recommends **Splink**, an open-source Python library developed by the UK Ministry of Justice, for this task. Splink is designed for speed and scalability (running on Spark or DuckDB) and utilizes the **Fellegi-Sunter model** of probabilistic linkage.

* **The EM Algorithm:** Splink uses the Expectation-Maximization (EM) algorithm to estimate the "m-probabilities" (probability that fields match given the records are a match) and "u-probabilities" (probability that fields match by random chance). This is crucial because, in a new company's data lake, the user likely lacks labeled "ground truth" data to train a supervised model. Splink learns the weights of the match parameters unsupervised.31  
* **Blocking:** To avoid the computational cost of comparing every record to every other record ($O(N^2)$), Splink uses "blocking rules." For example, it might only compare people who live in the same "Zip Code" or share the same "First 3 letters of Last Name."

#### **5.1.2 Zingg: Handling Messy Data**

An alternative or complement to Splink is **Zingg**, which focuses on building training sets for machine learning-based deduplication. Zingg is particularly effective when the data is "messy" (e.g., typos in names, address variations). It uses active learning to present uncertain pairs to a human user, rapidly training a model to recognize specific patterns of error within a company's data lake.32

### **5.2 The Specific Challenge of Hashed Data**

The user mentions hashed credit cards. This is a form of **Pseudonymization**. To link a "Hashed Credit Card" user from a POS system to an "Email" user from an Online Ordering (OLO) system, the architecture requires an **Identity Graph**.

* The Graph Data Structure:  
  An Identity Graph (often stored in a Graph Database like Neo4j or Amazon Neptune, or modeled in Snowflake) maps relationships between identifiers.34  
  * *Node Types:* Email, Phone, Hashed\_CC, Device\_ID, Cookie\_ID.  
  * *Edge Types:* Observed\_Together, Probabilistic\_Match.  
* Transitive Resolution:  
  If Customer A buys online using Email: jane@example.com and Phone: 555-0199.  
  Later, a customer in the store uses Phone: 555-0199 (for a loyalty lookup) and pays with Hash\_CC: 8x9s8d....  
  The graph transitively links Email: jane@example.com $\\leftrightarrow$ Phone: 555-0199 $\\leftrightarrow$ Hash\_CC: 8x9s8d....  
  Now, all POS transactions made with that credit card can be attributed to Jane's CLV, even though the POS system never captured her email.

### **5.3 Integrating Walled Gardens (Google and Meta)**

The user asks to relate this data to Google and Meta advertising. This is complex because these platforms are "Walled Gardens." You cannot simply download a table of "Users who saw my ad."

#### **5.3.1 Customer Match and CAPI**

To relate the data, you must *push* your identified segments to the platforms. This is known as **Customer Match** (Google) or **Conversions API / Custom Audiences** (Meta).

* **Mechanism:** You export the "Golden Record" attributes (Email, Phone, First Name, Last Name, Zip, Country) from your Unified Customer table.  
* **Hashing and Normalization:** Before transmission, this data *must* be normalized (lowercased, whitespace removed) and hashed using SHA-256. Research emphasizes that even a single trailing space results in a completely different hash, breaking the match.36  
* **The Feedback Loop:** Once the audiences are matched in the platform, you can view **aggregated** reporting on "Lift" or "Conversions" driven by campaigns targeting those specific segments. You can also use this for **Exclusion** (don't show ads to high-CLV customers who already buy frequently).

#### **5.3.2 Privacy Compliance**

An often-overlooked aspect of analyzing dimensionality is **PII Detection**. When automating the "selection of columns," the system must identify which columns contain sensitive data (PII). Tools like **Presidio** (Microsoft) or the PII detection features in **Databricks** can scan sampled data to tag columns as PII\_Email or PII\_Phone.7 This allows the pipeline to automatically apply hashing functions *before* the data enters the analytical layer, ensuring compliance (GDPR/CCPA) while maintaining linkability.

## ---

**6\. Integrating the Walled Gardens: Attribution & Match Rates**

The user specifically asks about relating customer spend to advertising from Google or Meta. This introduces the challenge of **Data Silos** and **Privacy Sandboxes**.

### **6.1 The "Match Key" Architecture**

You cannot simply "download" a table from Google Ads that says "User X saw Ad Y." Google and Meta provide aggregated data (Campaign level) or pseudonymized event-level data (Data Hubs).

To "relate" this data, two parallel streams are required:

1. **Inbound Attribution (Tagging/UTMs):** Ensuring the OLO and POS systems capture gclid (Google Click ID) or fbclid (Facebook Click ID) or, at minimum, UTM parameters (utm\_source, utm\_campaign). The Semantic Mapper must scan for these columns (often buried in JSON blobs or URL strings) and extract them into the Transaction Fact table.38  
2. **Outbound Enrichment (CAPI/Customer Match):** Sending the Global\_Customer\_ID and hashed PII *back* to the platforms via APIs (Meta CAPI, Google Customer Match). This allows the platforms to report "Lift" or "Conversions" against those audiences.37

### **6.2 Beyond Multi-Touch Attribution (MTA): The Rise of MMM**

Post-iOS14 (and with the deprecation of third-party cookies), tracking individual users across platforms has degraded. "Picking through data" to find a deterministic path from an Ad impression to a Store visit is increasingly impossible for a large portion of users.41

**Research Recommendation:** Do not rely solely on row-level attribution (MTA). Instead, implement **Media Mix Modeling (MMM)** using open-source libraries like **Robyn** (Meta) or **Meridian** (Google).43

* *Data Requirement:* MMM requires *aggregated* time-series data (e.g., "Daily Spend on FB", "Daily Impressions on Google") matched against "Daily Revenue" from the Transaction table.  
* *Integration:* The automated pipeline should aggregate the Transaction Fact table by day and region to create the input tensor for the MMM model. This allows you to estimate the *incremental* CLV contribution of Google/Meta without needing perfect user-level linkage.45

### **6.3 Triangulation**

The most sophisticated approach involves **Triangulation**: using MMM for high-level budget allocation, MTA (where data exists) for tactical optimization, and **Geo-Lift Experiments** to calibrate the models.47 The "Common Information Model" must support all three by maintaining both granular (user-level) and aggregated (date/cohort-level) tables.

## ---

**7\. Architecture Blueprint: The Composable Semantic Lakehouse**

To "quickly onboard" companies without manual engineering, the architecture must be modular, metadata-driven, and leverage the "Modern Data Stack." The recommended architecture is a **Composable CDP**.

### **7.1 The Stack Components**

1. **Ingestion Layer (EL):** Tools like **Fivetran** or **Airbyte** to dump raw data from CRM/POS/Ads into the Data Lake (Snowflake, Databricks, BigQuery). This is the "Bronze" layer—raw and untouched.49  
2. **Storage Layer (Data Lakehouse):** Storing data in open formats (Delta Lake, Iceberg) allows for schema evolution. If Company A adds a new column, the Lakehouse accommodates it without breaking the pipeline.7  
3. **Semantic Discovery & Transformation Layer (The "Brain"):**  
   * *Step 1: Profiling.* A Python worker runs **Sato/Sherlock** on the raw Bronze tables to infer semantic types.14  
   * *Step 2: Mapping Generation.* An **LLM Agent** reads the Sato profile and generates a **dbt (data build tool)** model. This dbt model defines the SQL SELECT statement that renames and casts the raw columns into the target CIM (Common Information Model).19  
   * *Step 3: Execution.* dbt executes these transformations to create the "Silver" layer (Standardized Data).  
4. **Identity Resolution Layer:**  
   * **Splink** runs on the Silver data to deduplicate users and assign Global\_Customer\_IDs. This creates the "Gold" layer (Unified Customer 360).31  
5. **Semantic Layer (Serving):**  
   * Tools like **Cube** or **dbt Semantic Layer** sit on top of the Gold data. They define metrics (e.g., clv, churn\_rate, cac) in code. This ensures that whether the user is in Tableau, Python, or a Custom App, the definition of "CLV" is consistent.51

### **7.2 Automation via "dbt-osmosis" and LLMs**

To achieve the speed the user requests ("quickly onboard"), the system should utilize **dbt-osmosis** or similar "yaml-generator" tools augmented by LLMs.

* *Workflow:* When a new tenant is added, the system scans the schema. The LLM predicts the YAML configuration (mapping cust\_email to email). A human engineer reviews the suggested YAML (reducing work from hours to minutes). Once approved, the pipeline runs automatically.53

## ---

**8\. Deep Insights and Strategic Recommendations**

### **8.1 Insight: The Shift from "Deterministic" to "Probabilistic" Truth**

The user’s request implies a desire for a "clean" answer ("determine what columns we need"). However, the research suggests that in a heterogeneous, multi-tenant environment, "truth" is probabilistic.

* *Implication:* The system should not reject data that doesn't fit perfectly. Instead, it should assign **Confidence Scores** to every mapping and every identity link. A CLV calculation should arguably include a confidence interval reflecting the quality of the underlying data linkage.

### **8.2 Insight: Privacy as an Architectural Constraint**

The mention of hashed credit cards and email addresses highlights a trend towards **Privacy-Preserving Analytics**.

* *Implication:* The data lake should be designed with "Data Clean Room" principles in mind. Identifying columns should be tokenized immediately upon ingestion. CLV analysis can often be performed on the tokenized data without ever decrypting the PII, reducing liability.

### **8.3 Insight: CLV is Dynamic, Not Static**

CLV is often treated as a historical reporting metric. However, the inclusion of Ad data suggests a need for *predictive* CLV to optimize ad spend (ROAS).

* *Implication:* The pipeline must not just "assemble data" but also "feed models." The architecture should automatically feature-engineer the data (creating the RFM vectors) and write them to a Feature Store, where pre-trained BTYD models can score customers daily.

## ---

**9\. Conclusion and Recommended Action Plan**

To solve the challenge of analyzing heterogeneous data lakes for CLV without manual "picking," the following roadmap is recommended based on the best available research:

1. **Adopt a "Schema-on-Read" Approach with AI Profiling:** Do not force companies to change their data exports. Ingest "as-is" and use **Sato** or **Sherlock** (Deep Learning) to automatically profile and tag columns with their semantic intent (e.g., "This looks like a Transaction Value").  
2. **Implement an LLM-Driven Transformation Layer:** Use Large Language Models to read these semantic tags and generate **dbt** code that maps the disparate source schemas to a **Common Information Model (CIM)**. This automates the "picking" of columns.  
3. **Deploy Probabilistic Identity Resolution:** Use **Splink** to link users across systems (POS, OLO, Ads) using fuzzy matching on available PII and hashed identifiers. Manage these links in an **Identity Graph**.  
4. **Standardize on the "Retail Star Schema":** Force the output of the transformation layer into a strict schema (Customer, Transaction, Product, Touchpoint) that supports **RFM** and **BTYD** predictive models.  
5. **Triangulate Attribution:** Integrate **Media Mix Modeling (Robyn/Meridian)** using the aggregated data from your standardized schema to measure the impact of Google/Meta ads, rather than relying solely on fragile user-level tracking.

By treating the schema mapping problem as a machine learning classification task rather than a manual engineering task, you can achieve the scale and speed required to onboard multiple companies efficiently.

#### **Works cited**

1. Q\&A: Understanding Retail Data Schema Design \- Retlia, accessed November 30, 2025, [https://retlia.com/retail-data/qa-understanding-retail-data-schema-design/](https://retlia.com/retail-data/qa-understanding-retail-data-schema-design/)  
2. Need Help: Building Star Schema for Customer Retention & Lifetime Value Analytics, accessed November 30, 2025, [https://www.reddit.com/r/dataengineering/comments/1fxgmr4/need\_help\_building\_star\_schema\_for\_customer/](https://www.reddit.com/r/dataengineering/comments/1fxgmr4/need_help_building_star_schema_for_customer/)  
3. Chorus: Foundation Models for Unified Data Discovery and Exploration \- ResearchGate, accessed November 30, 2025, [https://www.researchgate.net/publication/381085947\_Chorus\_Foundation\_Models\_for\_Unified\_Data\_Discovery\_and\_Exploration](https://www.researchgate.net/publication/381085947_Chorus_Foundation_Models_for_Unified_Data_Discovery_and_Exploration)  
4. Sherlock: A Deep Learning Approach to Semantic Data Type Detection \- ResearchGate, accessed November 30, 2025, [https://www.researchgate.net/publication/350006666\_Sherlock\_A\_Deep\_Learning\_Approach\_to\_Semantic\_Data\_Type\_Detection](https://www.researchgate.net/publication/350006666_Sherlock_A_Deep_Learning_Approach_to_Semantic_Data_Type_Detection)  
5. Data Integration and Storage Strategies in Heterogeneous Analytical Systems: Architectures, Methods, and Interoperability Challenges \- MDPI, accessed November 30, 2025, [https://www.mdpi.com/2078-2489/16/11/932](https://www.mdpi.com/2078-2489/16/11/932)  
6. Heterogeneity in Entity Matching: A Survey and Experimental Analysis \- arXiv, accessed November 30, 2025, [https://arxiv.org/html/2508.08076v1](https://arxiv.org/html/2508.08076v1)  
7. The Beacon Architecture: Rethinking multi-tenant security data operations for MSSPs, accessed November 30, 2025, [https://www.databahn.ai/blog/the-beacon-architecture-rethinking-multi-tenant-security-data-operations-for-mssps](https://www.databahn.ai/blog/the-beacon-architecture-rethinking-multi-tenant-security-data-operations-for-mssps)  
8. Delta Lake Deep Dive: The Complete Guide to Modern Data Lake Architecture \- Medium, accessed November 30, 2025, [https://medium.com/@diwasb54/delta-lake-deep-dive-the-complete-guide-to-modern-data-lake-architecture-2c5b5c4c1ecf](https://medium.com/@diwasb54/delta-lake-deep-dive-the-complete-guide-to-modern-data-lake-architecture-2c5b5c4c1ecf)  
9. Guarding Digital Privacy: Exploring User Profiling and Security Enhancements, accessed November 30, 2025, [https://www.researchgate.net/publication/390671855\_Guarding\_Digital\_Privacy\_Exploring\_User\_Profiling\_and\_Security\_Enhancements](https://www.researchgate.net/publication/390671855_Guarding_Digital_Privacy_Exploring_User_Profiling_and_Security_Enhancements)  
10. EP2777182A1 \- Identifying a same user of multiple communication devices based on web page visits, application usage, location, or route \- Google Patents, accessed November 30, 2025, [https://patents.google.com/patent/EP2777182A1/en](https://patents.google.com/patent/EP2777182A1/en)  
11. Sherlock: A Deep Learning Approach to Semantic Data Type Detection, accessed November 30, 2025, [https://sherlock.media.mit.edu/](https://sherlock.media.mit.edu/)  
12. \[1905.10688\] Sherlock: A Deep Learning Approach to Semantic Data Type Detection \- arXiv, accessed November 30, 2025, [https://arxiv.org/abs/1905.10688](https://arxiv.org/abs/1905.10688)  
13. megagonlabs/sato: Code and data for Sato https://arxiv.org/abs/1911.06311. \- GitHub, accessed November 30, 2025, [https://github.com/megagonlabs/sato](https://github.com/megagonlabs/sato)  
14. Sato: Contextual Semantic Type Detection in Tables \- VLDB Endowment, accessed November 30, 2025, [https://www.vldb.org/pvldb/vol13/p1835-zhang.pdf](https://www.vldb.org/pvldb/vol13/p1835-zhang.pdf)  
15. Sato: Contextual Semantic Type Detection in Tables \- arXiv, accessed November 30, 2025, [https://www.arxiv.org/pdf/1911.06311v2](https://www.arxiv.org/pdf/1911.06311v2)  
16. Prompt engineering for foundation models \- Amazon SageMaker AI, accessed November 30, 2025, [https://docs.aws.amazon.com/sagemaker/latest/dg/jumpstart-foundation-models-customize-prompt-engineering.html](https://docs.aws.amazon.com/sagemaker/latest/dg/jumpstart-foundation-models-customize-prompt-engineering.html)  
17. Dynamic Schema-Aware Prompting in LLMs \- Emergent Mind, accessed November 30, 2025, [https://www.emergentmind.com/topics/dynamic-schema-aware-prompting](https://www.emergentmind.com/topics/dynamic-schema-aware-prompting)  
18. I built a Python tool to create a semantic layer over SQL for LLMs using a Knowledge Graph. Is this a useful approach? : r/dataengineering \- Reddit, accessed November 30, 2025, [https://www.reddit.com/r/dataengineering/comments/1n81kxy/i\_built\_a\_python\_tool\_to\_create\_a\_semantic\_layer/](https://www.reddit.com/r/dataengineering/comments/1n81kxy/i_built_a_python_tool_to_create_a_semantic_layer/)  
19. A new era of data engineering: dbt Copilot is GA, accessed November 30, 2025, [https://www.getdbt.com/blog/dbt-copilot-is-ga](https://www.getdbt.com/blog/dbt-copilot-is-ga)  
20. LLM-Powered SQL: Can AI Write Reliable Data Models in Production? | by Manik Hossain, accessed November 30, 2025, [https://medium.com/@manik.ruet08/llm-powered-sql-can-ai-write-reliable-data-models-in-production-d70fae82793c](https://medium.com/@manik.ruet08/llm-powered-sql-can-ai-write-reliable-data-models-in-production-d70fae82793c)  
21. Valentine: Evaluating Matching Techniques for Dataset Discovery \- GitHub Pages, accessed November 30, 2025, [https://delftdata.github.io/valentine/](https://delftdata.github.io/valentine/)  
22. Valentine in Action: Matching Tabular Data at Scale \- VLDB Endowment, accessed November 30, 2025, [http://vldb.org/pvldb/vol14/p2871-koutras.pdf](http://vldb.org/pvldb/vol14/p2871-koutras.pdf)  
23. Prompt Engineering Deep Dive: Mastering the Art for LLMs and VLMs | by Shawn | Medium, accessed November 30, 2025, [https://medium.com/@hexiangnan/prompt-engineering-deep-dive-mastering-the-art-for-llms-and-vlms-40bff49a3800](https://medium.com/@hexiangnan/prompt-engineering-deep-dive-mastering-the-art-for-llms-and-vlms-40bff49a3800)  
24. Valentine: Evaluating Matching Techniques for Dataset Discovery \- Department of information engineering and computer science, accessed November 30, 2025, [http://disi.unitn.it/\~pavel/OM/articles/Koutras\_ICDE21.pdf](http://disi.unitn.it/~pavel/OM/articles/Koutras_ICDE21.pdf)  
25. CLV Part 2: Estimating Future Spend \- Databricks, accessed November 30, 2025, [https://www.databricks.com/notebooks/clv\_part2\_estimating\_future\_spend.html](https://www.databricks.com/notebooks/clv_part2_estimating_future_spend.html)  
26. CLV Quickstart — Open Source Marketing Analytics Solution, accessed November 30, 2025, [https://www.pymc-marketing.io/en/latest/notebooks/clv/clv\_quickstart.html](https://www.pymc-marketing.io/en/latest/notebooks/clv/clv_quickstart.html)  
27. RetailChannelTable in Main \- Common Data Model \- Microsoft Learn, accessed November 30, 2025, [https://learn.microsoft.com/en-us/common-data-model/schema/core/operationscommon/tables/commerce/channelmanagement/main/retailchanneltable](https://learn.microsoft.com/en-us/common-data-model/schema/core/operationscommon/tables/commerce/channelmanagement/main/retailchanneltable)  
28. Difference between Star Schema and Snowflake Schema \- GeeksforGeeks, accessed November 30, 2025, [https://www.geeksforgeeks.org/dbms/difference-between-star-schema-and-snowflake-schema/](https://www.geeksforgeeks.org/dbms/difference-between-star-schema-and-snowflake-schema/)  
29. Probabilistic Record Linkage with Splink \- IN.gov, accessed November 30, 2025, [https://www.in.gov/mph/files/Probabilistic-Record-Linkage-with-Splink-State-Health-Simmons.pdf](https://www.in.gov/mph/files/Probabilistic-Record-Linkage-with-Splink-State-Health-Simmons.pdf)  
30. Splink: Fast, accurate and scalable record linkage \- Data in government \- GOV.UK blogs, accessed November 30, 2025, [https://dataingovernment.blog.gov.uk/2022/09/23/splink-fast-accurate-and-scalable-record-linkage/](https://dataingovernment.blog.gov.uk/2022/09/23/splink-fast-accurate-and-scalable-record-linkage/)  
31. Splink, accessed November 30, 2025, [https://moj-analytical-services.github.io/splink/index.html](https://moj-analytical-services.github.io/splink/index.html)  
32. zinggAI/zingg: Scalable identity resolution, entity resolution, data mastering and deduplication using ML \- GitHub, accessed November 30, 2025, [https://github.com/zinggAI/zingg](https://github.com/zinggAI/zingg)  
33. Step by Step Identity Resolution With Zingg on Databricks | by Sonal Goyal \- Medium, accessed November 30, 2025, [https://medium.com/@sonalgoyal/step-by-step-identity-resolution-with-zingg-on-databricks-850b8f4b8198](https://medium.com/@sonalgoyal/step-by-step-identity-resolution-with-zingg-on-databricks-850b8f4b8198)  
34. What Is an Identity Graph? Real-Time Use Cases | Aerospike, accessed November 30, 2025, [https://aerospike.com/blog/what-is-identity-graph/](https://aerospike.com/blog/what-is-identity-graph/)  
35. Graph Databases for Identity & Access Management Uses Cases \- Neo4j, accessed November 30, 2025, [https://neo4j.com/use-cases/identity-and-access-management/](https://neo4j.com/use-cases/identity-and-access-management/)  
36. Prepare your data for import \- Google Ads Data Manager Help, accessed November 30, 2025, [https://support.google.com/google-ads-data-manager/answer/14184381?hl=en](https://support.google.com/google-ads-data-manager/answer/14184381?hl=en)  
37. Conversions API for Server-Side Google Tag Manager (GTM) \- Meta for Developers, accessed November 30, 2025, [https://developers.facebook.com/docs/marketing-api/conversions-api/guides/gtm-server-side/](https://developers.facebook.com/docs/marketing-api/conversions-api/guides/gtm-server-side/)  
38. Google Ads Data Manager: Your Guide to Smarter Advertising \- Linear, accessed November 30, 2025, [https://lineardesign.com/blog/google-ads-data-manager/](https://lineardesign.com/blog/google-ads-data-manager/)  
39. Modeling marketing attribution \- dbt Labs, accessed November 30, 2025, [https://www.getdbt.com/blog/modeling-marketing-attribution](https://www.getdbt.com/blog/modeling-marketing-attribution)  
40. How to improve event match quality with the first party data \- EasyInsights, accessed November 30, 2025, [https://easyinsights.ai/blog/improve-event-match-quality-with-first-party-data/](https://easyinsights.ai/blog/improve-event-match-quality-with-first-party-data/)  
41. 15 Best Ad Tech Platforms for Attribution Modeling in 2025 \- Madgicx, accessed November 30, 2025, [https://madgicx.com/blog/ad-tech-platform-for-attribution-modeling](https://madgicx.com/blog/ad-tech-platform-for-attribution-modeling)  
42. Incrementality ≠ Last-Click: The 7 Myths Holding Mobile UA Back \- Appier, accessed November 30, 2025, [https://www.appier.com/en/blog/incrementality-last-click-the-7-myths-holding-mobile-ua-back](https://www.appier.com/en/blog/incrementality-last-click-the-7-myths-holding-mobile-ua-back)  
43. Meridian is an MMM framework that enables advertisers to set up and run their own in-house models. \- GitHub, accessed November 30, 2025, [https://github.com/google/meridian](https://github.com/google/meridian)  
44. Media Mix Modeling MMM: The Marketing Revolution that no one talks about, accessed November 30, 2025, [https://www.deepmarketing.it/en/post/media-mix-modeling-the-marketing-revolution-that-no-one-talks-about](https://www.deepmarketing.it/en/post/media-mix-modeling-the-marketing-revolution-that-no-one-talks-about)  
45. MMM: Bayesian Framework for Marketing Mix Modeling and ROAS \- Towards Data Science, accessed November 30, 2025, [https://towardsdatascience.com/mmm-bayesian-framework-for-marketing-mix-modeling-and-roas-ccade4005bd5/](https://towardsdatascience.com/mmm-bayesian-framework-for-marketing-mix-modeling-and-roas-ccade4005bd5/)  
46. How to Use Marketing Mix Modeling to Optimize Your Marketing Strategy \- Strong Analytics, accessed November 30, 2025, [https://www.strong.io/blog/how-to-use-marketing-mix-modeling-to-optimize-your-marketing-strategy](https://www.strong.io/blog/how-to-use-marketing-mix-modeling-to-optimize-your-marketing-strategy)  
47. Marketing Mix Modeling: A Complete Guide for Strategic Marketers | Measured®, accessed November 30, 2025, [https://www.measured.com/faq/marketing-mix-modeling-2025-complete-guide-for-strategic-marketers/](https://www.measured.com/faq/marketing-mix-modeling-2025-complete-guide-for-strategic-marketers/)  
48. What is Marketing Effectiveness and How to Measure it? \- Lifesight, accessed November 30, 2025, [https://lifesight.io/blog/marketing-effectiveness/](https://lifesight.io/blog/marketing-effectiveness/)  
49. rittmananalytics/ra\_attribution: Example Multi-Cycle, Multi-Touch Revenue and Cost Attribution Model \- GitHub, accessed November 30, 2025, [https://github.com/rittmananalytics/ra\_attribution](https://github.com/rittmananalytics/ra_attribution)  
50. What is Conceptual Data Modeling: Purpose & Examples \- Airbyte, accessed November 30, 2025, [https://airbyte.com/data-engineering-resources/conceptual-data-model](https://airbyte.com/data-engineering-resources/conceptual-data-model)  
51. Understanding semantic layer architecture | dbt Labs, accessed November 30, 2025, [https://www.getdbt.com/blog/semantic-layer-architecture](https://www.getdbt.com/blog/semantic-layer-architecture)  
52. Semantic Layer: The Backbone of AI-powered Data Experiences \- Cube Blog, accessed November 30, 2025, [https://cube.dev/blog/semantic-layer-the-backbone-of-ai-powered-data-experiences](https://cube.dev/blog/semantic-layer-the-backbone-of-ai-powered-data-experiences)  
53. z3z1ma/dbt-osmosis: Provides automated YAML management and a streamlit workbench. Designed to optimize dev workflows. \- GitHub, accessed November 30, 2025, [https://github.com/z3z1ma/dbt-osmosis](https://github.com/z3z1ma/dbt-osmosis)  
54. Automate Sensitive Data Protection With Metadata-Driven Masking \- Xebia, accessed November 30, 2025, [https://xebia.com/blog/automate-sensitive-data-protection-with-metadata-driven-masking/](https://xebia.com/blog/automate-sensitive-data-protection-with-metadata-driven-masking/)
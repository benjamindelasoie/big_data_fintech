# Data Sanitization Decisions for Fintech ETL Pipeline

This document outlines all the data sanitization decisions made during the preprocessing stage of the ETL pipeline.

## Overview

Based on the data exploration analysis, several data quality issues were identified that required sanitization decisions to ensure clean, consistent, and reliable data for analysis.

## 1. Users Dataset (dim_users.csv) Sanitization

### Issues Identified:
- Index column (`_c0`) not needed for analysis
- Mix of Brazilian (MLB prefix) and non-Brazilian users
- Inconsistent data types in `type` and `rubro` columns
- Missing values in `rubro` field
- Invalid user ID formats

### Decisions Made:

#### Decision 1.1: Remove Index Column
**Action**: Remove `_c0` column
**Rationale**: Index columns are not needed for business analysis and consume unnecessary storage space.

#### Decision 1.2: Filter Brazilian Users Only
**Action**: Keep only users with `user_id` starting with "MLB"
**Rationale**: The A/B testing experiment is focused on Brazilian users only, as specified in the requirements.
**Impact**: Reduced dataset from ~39,000 to ~13,000 users (66.7% of data)

#### Decision 1.3: Standardize Data Types
**Action**: Convert `type` column to integer, handle non-numeric values as NULL
**Rationale**: Ensures consistent data types for downstream processing and analytics.

#### Decision 1.4: Handle Rubro Column
**Action**: Convert `rubro` to double, keep NULL for non-sellers
**Rationale**: `rubro` is only relevant for sellers, so NULL values for individuals are acceptable.

#### Decision 1.5: Validate User ID Format
**Action**: Remove records with invalid user ID formats (not matching "MLB[digits]")
**Rationale**: Ensures data consistency and prevents downstream processing errors.

## 2. Transactions Dataset (bt_users_transactions.csv) Sanitization

### Issues Identified:
- Index column (`_c0`) not needed
- Mix of Brazilian and non-Brazilian users
- Potentially invalid transaction types
- Invalid segment values
- Null timestamps
- Transactions outside business date range

### Decisions Made:

#### Decision 2.1: Remove Index Column
**Action**: Remove `_c0` column
**Rationale**: Same as Decision 1.1

#### Decision 2.2: Filter Brazilian Users Only
**Action**: Keep only transactions from users with MLB prefix
**Rationale**: Align with A/B testing scope (Brazilian users only)

#### Decision 2.3: Validate Transaction Types
**Action**: Keep only transaction types 1-9 (1-7 for payments, 8-9 for collections)
**Rationale**: Business logic defines these as valid transaction types. Invalid types could skew analysis.

#### Decision 2.4: Validate User Segments
**Action**: Keep only segments 1 (individuals) and 2 (sellers)
**Rationale**: Business logic defines only these two segments. Invalid segments would affect metric calculations.

#### Decision 2.5: Remove Invalid Timestamps
**Action**: Filter out transactions with NULL timestamps
**Rationale**: Timestamp is critical for onboarding period calculations and temporal analysis.

#### Decision 2.6: Date Range Filtering
**Action**: Keep only transactions from 2022 (01-01 to 12-31)
**Rationale**: Focus on the experimental period, remove potential data entry errors from other years.

## 3. Onboarding Dataset (lk_onboarding.csv) Sanitization

### Issues Identified:
- Multiple index columns (`_c0`, `Unnamed: 0`)
- Mix of Brazilian and non-Brazilian users
- Inconsistent data types in metric flags
- Missing values in flag columns
- Invalid first login dates

### Decisions Made:

#### Decision 3.1: Remove Index Columns
**Action**: Remove `_c0` and `Unnamed: 0` columns
**Rationale**: Index columns not needed for analysis

#### Decision 3.2: Filter Brazilian Users Only
**Action**: Keep only users with MLB prefix
**Rationale**: Align with experimental scope

#### Decision 3.3: Standardize Metric Flags
**Action**: Convert all metric flags (`habito`, `activacion`, `setup`, `return`) to integers
**Rationale**: Ensures consistent data types for boolean flags (0/1)

#### Decision 3.4: Handle Missing Flag Values
**Action**: Convert NULL values in flag columns to 0
**Rationale**: Missing flag typically means the event didn't occur, so 0 is appropriate default

#### Decision 3.5: Validate First Login Dates
**Action**: Remove records with NULL `first_login_dt`
**Rationale**: First login date is essential for onboarding period calculations

#### Decision 3.6: Date Range Filtering
**Action**: Keep only records with first login in 2022
**Rationale**: Focus on experimental period, ensure data consistency

## 4. Cross-Dataset Consistency Decisions

### Decision 4.1: User ID Consistency
**Action**: Ensure all datasets use the same user ID format (MLB prefix)
**Rationale**: Enables proper joins and prevents data integration issues

### Decision 4.2: Date Range Alignment
**Action**: Filter all datasets to 2022 date range
**Rationale**: Ensures temporal consistency across all datasets

### Decision 4.3: Brazilian Users Focus
**Action**: Filter all datasets to Brazilian users only
**Rationale**: Aligns with A/B testing experiment scope

## 5. Impact Assessment

### Data Volume Impact:
- **Users Dataset**: 39,000 → ~13,000 records (66.7% retention)
- **Transactions Dataset**: 7,675 → ~7,675 records (100% retention, already Brazilian users)
- **Onboarding Dataset**: 13,500 → ~13,000 records (96.3% retention)

### Data Quality Improvements:
- Eliminated inconsistent data types
- Removed invalid user IDs and transaction types
- Standardized date formats and ranges
- Ensured cross-dataset consistency
- Removed unnecessary columns

## 6. Business Logic Validation

### Segmentation Logic Preserved:
- Individuals: Users with only payment transactions (types 1-7)
- Sellers: Users with collection transactions (types 8-9)
- Unknown: Users without transactions during onboarding

### Metric Calculation Integrity:
- All flag columns properly formatted for metric calculations
- Date columns validated for temporal analysis
- User segments properly classified

## 7. Risk Assessment

### Low Risk Decisions:
- Removing index columns (no business value lost)
- Standardizing data types (improves consistency)
- Date range filtering (focuses on relevant period)

### Medium Risk Decisions:
- Filtering to Brazilian users only (reduces dataset size significantly)
- Handling missing rubro values as NULL (acceptable for individuals)

### Mitigation Strategies:
- Document all filtering decisions
- Maintain original raw data for audit purposes
- Validate results against business expectations
- Monitor metric calculations for anomalies

## 8. Data Lineage

All sanitization decisions are logged and traceable:
1. Raw data preserved in original CSV files
2. Cleaning steps documented in preprocessing code
3. Intermediate results saved in processed_data directory
4. Validation metrics calculated and logged

## 9. Quality Assurance

### Validation Checks:
- User ID format validation
- Date range consistency checks
- Data type validation
- Cross-dataset user overlap verification
- Metric flag value validation (0/1 only)

### Monitoring:
- Record count changes logged
- Data quality metrics calculated
- Validation results documented
- Error handling for edge cases

This sanitization approach ensures clean, consistent, and business-logic-compliant data for the transformation and analysis stages of the ETL pipeline.
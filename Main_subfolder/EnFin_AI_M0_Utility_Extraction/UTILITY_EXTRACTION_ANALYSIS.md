# Utility Bill Data Extraction Analysis

## Executive Summary

This document provides a detailed analysis of the data fields to be extracted from utility bills for the VPP data tape generation system. Based on visual inspection of sample bills from 5 utilities across California and Illinois, this analysis maps exact field locations, formats, and extraction strategies.

---

## Requirements Overview

| Program | Utility | Required Fields |
|---------|---------|-----------------|
| CA ELRP | PG&E, SCE, SDG&E | Meter ID |
| IL PJM | ComED | Account Number, Electric Choice ID, Meter Number |
| IL MISO | Ameren | Account Number, Meter Number |

---

## California Utilities (CA ELRP Program)

### 1. PG&E (Pacific Gas & Electric)

**Sample Bill Analysis:**

| Field | Value Found | Location | Format |
|-------|-------------|----------|--------|
| Account Number | 4479518448-8 | Page 1 - Top right header | 10 digits with check digit (XXXXXXXXXX-X) |
| Service Agreement ID | 4472676872 | Page 3 - "Details of Electric Charges" section | 10 digits |
| **Meter ID (Electric)** | **1011207547** | **Page 3 - "Service Information" section, labeled "Meter #"** | **10 digits** |
| Meter ID (Gas) | 31665876 | Page 4 - "Service Information" section | 8 digits |

**Extraction Details for Meter ID:**
- **Location**: Page 3 (Electric) or Page 4 (Gas)
- **Section Header**: "Service Information"
- **Label**: "Meter #"
- **Format**: 10-digit numeric string
- **Visual Context**: Right side of page, in a box with other service details like "Total Usage", "Baseline Territory", "Heat Source"

**Additional Fields Available:**
- Service Address: 4749 MATTERHORN WAY
- Rate Schedule: Time-of-Use (Peak Pricing 4-9 p.m. Every Day)
- Baseline Territory: S
- Total Usage: 299.443800 kWh

---

### 2. SCE (Southern California Edison)

**Sample Bill Analysis:**

| Field | Value Found | Location | Format |
|-------|-------------|----------|--------|
| Customer Account | 7070410384 | Page 1 - Top left, "Customer account" | 10 digits |
| Service Account | 0700190054528643 | Page 1 - "Service account" field | 16 digits |
| POD ID | (visible in bill) | Page 1 - Near account info | Variable |
| **Meter ID** | **2220 1 4459319** | **Page 3 - "Your past and current electricity usage" section** | **Smart meter format starting with 222** |

**Extraction Details for Meter ID:**
- **Location**: Page 3
- **Section Header**: "Your past and current electricity usage" or detailed billing section
- **Label**: Often appears in usage details table
- **Format**: SCE smart meters typically start with "222" prefix
- **Visual Context**: In the electricity usage breakdown section

**Additional Fields Available:**
- Service Address: 3941 H SALINAS DR, MURRIETA CA 92563
- Rate: TOU-D-4-9PM
- Billing Period: 09/26/24 to 10/17/24
- Total Usage: 1,688 kWh

---

### 3. SDG&E (San Diego Gas & Electric)

**Sample Bill Analysis:**

| Field | Value Found | Location | Format |
|-------|-------------|----------|--------|
| Account Number | 0098 5273 6232 2 | Page 1 - Top header, "ACCOUNT NUMBER" | Format: XXXX XXXX XXXX X |
| Service Address | 23062 SANTO DR, MISSION VIEJO, CA 92691 | Page 1 - "SERVICE FOR" section | Address format |
| **Meter ID** | **06230504** | **Page 3 - "Detail of Current Charges" section, "Meter Number"** | **8 digits, often starting with 0** |

**Extraction Details for Meter ID:**
- **Location**: Page 3
- **Section Header**: "Detail of Current Charges"
- **Sub-section**: "Electric Service"
- **Label**: "Meter Number:"
- **Format**: 8-digit number, often starts with "0" (must include leading zero)
- **Visual Context**: At the top of the "Electric Service" section with billing period, baseline allowance, etc.

**Additional Fields Available:**
- Rate/Tariff: TCA-2P1 Residential
- Billing Period: 1/6/25 - 2/5/25
- Baseline Allowance: 267 kWh
- Climate Zone: Coastal

---

## Illinois Utilities

### 4. ComED (Commonwealth Edison) - IL PJM

**Sample Bill Analysis:**

| Field | Value Found | Location | Format |
|-------|-------------|----------|--------|
| **Account Number** | **2093796000** | **Page 1 - Top right "Account #" AND Page 2 header** | **10 digits** |
| **Electric Choice ID** | **0090604802** | **Page 2 - "SERVICE ADDRESS" section, labeled "Electric Choice ID"** | **10 digits** |
| **Meter Number** | **37168646** | **Page 2 - "METER INFORMATION" section** | **8 digits** |

**Extraction Details:**

**Account Number:**
- **Location**: Page 1 (top right) and Page 2 (header)
- **Label**: "Account #" or "Account"
- **Format**: 10-digit numeric string
- **Visual Context**: Prominently displayed in header area

**Electric Choice ID:**
- **Location**: Page 2
- **Section Header**: "SERVICE ADDRESS" box
- **Label**: "Electric Choice ID:"
- **Format**: 10-digit numeric string
- **Visual Context**: Inside a blue/teal service address box, below the street address

**Meter Number:**
- **Location**: Page 2
- **Section Header**: "METER INFORMATION"
- **Label**: In table with columns: Meter Number, Load Type, Reading Type, Total kWh, etc.
- **Format**: 8-digit numeric string
- **Visual Context**: First column in the meter information table

**Additional Fields Available:**
- Service Address: 4201 N Sherard St, Chicago, IL 60618
- Meter Load Type: 37168646 (same as meter number in this case)
- Reading Type: General Service
- Billing Period: 10/18/24 - 11/18/24

---

### 5. Ameren Illinois - IL MISO

**Sample Bill Analysis:**

| Field | Value Found | Location | Format |
|-------|-------------|----------|--------|
| **Account Number** | **1870008011** | **Page 1 - Top left, "Account Number:"** | **10 digits** |
| Customer Name | SARAH J TESTER | Page 1 - "Customer Name:" | Text |
| Service Address | 804 HOLIDAY DR, CHAMPAIGN, IL 61821 | Page 1 - "Service Address:" | Address format |
| **Meter Number** | **72253996** | **Page 3 - "Electric Meter Read" table, "METER NUMBER" column** | **8 digits** |

**Extraction Details:**

**Account Number:**
- **Location**: Page 1 (top left header area)
- **Label**: "Account Number:"
- **Format**: 10-digit numeric string
- **Visual Context**: Part of customer information block at top of bill

**Meter Number:**
- **Location**: Page 3
- **Section Header**: "Electric Meter Read for 04/01/2025 - 04/30/2025"
- **Table Columns**: READ TYPE | METER NUMBER | CURRENT METER READ | PREVIOUS METER READ | READ DIFFERENCE | MULTIPLIER | USAGE
- **Label**: "METER NUMBER" column
- **Format**: 8-digit numeric string
- **Visual Context**: In a detailed usage table, green highlighted header row

**Additional Fields Available:**
- Statement Issued: 05/06/2025
- Customer Service: 1-800-755-5000
- Billing Period: 04/01/2025 - 04/30/2025
- Total Electric Charge: $91.15
- Total Gas Charge: $57.87

---

## Extraction Strategy Summary

### Field Location Matrix

| Utility | Account Number | Electric Choice ID | Meter Number/ID |
|---------|---------------|-------------------|-----------------|
| PG&E | Page 1, Header | N/A | Page 3, "Service Information" |
| SCE | Page 1, "Customer account" | N/A | Page 3, Usage section |
| SDG&E | Page 1, Header | N/A | Page 3, "Detail of Current Charges" |
| ComED | Page 1-2, Header | Page 2, "SERVICE ADDRESS" box | Page 2, "METER INFORMATION" table |
| Ameren | Page 1, Header | N/A | Page 3, "Electric Meter Read" table |

### Field Format Summary

| Field | Format | Validation Regex |
|-------|--------|------------------|
| PG&E Meter ID | 10 digits | `^\d{10}$` |
| SCE Meter ID | 10-14 digits, starts with 222 | `^222\d{7,11}$` |
| SDG&E Meter Number | 8 digits (keep leading zeros) | `^\d{8}$` |
| ComED Account Number | 10 digits | `^\d{10}$` |
| ComED Electric Choice ID | 10 digits | `^\d{10}$` |
| ComED Meter Number | 8 digits | `^\d{8}$` |
| Ameren Account Number | 10 digits | `^\d{10}$` |
| Ameren Meter Number | 8 digits | `^\d{8}$` |

---

## Recommended Extraction Approach

### For AI/ML Extraction (Bedrock Llama 4 Maverick)

1. **Document Classification**: First identify the utility provider from:
   - Logo detection (PG&E, SCE, SDG&E, ComED, Ameren logos)
   - Header text patterns
   - Service address state (CA vs IL)

2. **Section Localization**: Navigate to the correct page/section based on utility type:
   - CA utilities: Focus on pages 3-4 for meter information
   - IL utilities: Focus on pages 1-3 for account and meter details

3. **Field Extraction**: Use targeted prompts based on utility type with specific labels to look for

4. **Validation**: Apply format validation rules to ensure data quality (>95% accuracy target)

### Prompt Engineering Strategy

For each utility, the extraction prompt should include:
- Specific section names to locate
- Exact label text to search for
- Expected format/pattern
- Fallback locations if primary location fails

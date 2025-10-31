# Databricks DLT Generator - Unified Edition

## Überblick

Dieser unified Generator kombiniert die besten Features der vorhandenen Prototypen:

- **Design**: Professional Enterprise-Design von `optimized-csv-generator (1).html`
- **Git-Integration**: Robuste Template-Verwaltung von beiden Generatoren
- **Pattern-Helper**: Regex-Builder für Data Quality Rules
- **DLT Code-Generierung**: Databricks Delta Live Tables mit Expectations
- **Boolean-Cleansing**: Automatische Integration der `flag_x` Helper-Funktion

---

## Features

### ✨ Hauptfeatures

1. **3-Schritt-Workflow**
   - Upload & Git-Konfiguration
   - Spalten-Konfiguration mit Data Types
   - Code-Generierung mit Live-Preview

2. **Git Template Integration**
   - Laden von Cleansing-Templates aus öffentlichen GitHub-Repositories
   - Automatische Integration in generierte Pipelines
   - localStorage-Persistenz für wiederholte Nutzung

3. **Boolean-Cleansing mit Helper-Funktionen**
   - Automatisches Einfügen der `flag_x` Funktion aus Git-Templates
   - Anwendung auf alle als "Boolean" markierten Spalten
   - Saubere Trennung zwischen Helper-Functions und DLT-Code

4. **Data Quality Expectations**
   - NOT NULL Checks
   - UNIQUE Constraints (als NOT NULL implementiert)
   - Pattern-Validierung mit Regex
   - Pattern-Helper für häufige Patterns (E-Mail, Telefon, PLZ, etc.)

5. **Professional Code-Output**
   - Saubere Code-Struktur mit Sections
   - Type Hints (DataFrame, Optional)
   - Docstrings mit Metadaten
   - PEP 8 konform

---

## Verwendung

### 1. CSV-Datei hochladen

```
Tab 1: Upload & Git
├── CSV-Datei auswählen (z.B. test_data.csv)
└── Automatisches Parsing mit PapaParse
```

### 2. Git-Templates laden (Optional)

**Repository-Struktur:**
```
your-repo/
└── cleansing_templates/
    ├── boolean_cleansing.py
    ├── email_validation.py
    └── date_formatting.py
```

**Konfiguration:**
- Repository URL: `https://github.com/your-username/your-repo`
- Branch: `main` (oder beliebiger Branch)
- Klick auf "Templates laden"

**Beispiel boolean_cleansing.py:**
```python
from pyspark.sql import functions as F

def flag_x(df, column_name):
    """
    Ersetzt in der angegebenen Spalte Leerzeichen (" " oder leere Strings) durch False
    und jedes 'X' oder 'x' durch True.
    """
    return (
        df.withColumn(
            column_name,
            F.when(
                F.lower(F.trim(F.col(column_name))) == "x",
                F.lit(True)
            ).otherwise(F.lit(False))
        )
    )
```

### 3. Spalten konfigurieren

```
Tab 2: Konfiguration
├── Tabellen-Metadaten
│   ├── Name: customer_data
│   └── Beschreibung: Kundendaten mit Quality Checks
├── Spalten-Mapping
│   ├── CUSTOMER_ID → customer_id (Integer)
│   ├── IS_ACTIVE_FLAG → is_active (String) ☑ Boolean
│   └── CONSENT_FLAG → consent_given (String) ☑ Boolean
└── Expectations
    ├── customer_id: NOT NULL
    ├── email: Pattern (E-Mail-Regex)
    └── is_active: NOT NULL
```

### 4. Code generieren

```
Tab 3: Generierung
├── Summary-Cards (Überblick)
├── "Python-Code generieren" klicken
└── Download oder Copy to Clipboard
```

---

## Generierter Code-Struktur

### Beispiel-Output

```python
import dlt
from pyspark.sql import DataFrame, functions as F
from typing import Optional

# ============================================
# HELPER FUNCTIONS
# ============================================

def flag_x(df, column_name):
    """
    Ersetzt in der angegebenen Spalte Leerzeichen durch False
    und jedes 'X' oder 'x' durch True.
    """
    return (
        df.withColumn(
            column_name,
            F.when(
                F.lower(F.trim(F.col(column_name))) == "x",
                F.lit(True)
            ).otherwise(F.lit(False))
        )
    )

# ============================================
# DLT TABLE DEFINITION
# ============================================

@dlt.table(
    name="customer_data",
    comment="Kundendaten mit Quality Checks"
)
@dlt.expect_all_or_drop({
    "valid_customer_id_0": "customer_id IS NOT NULL",
    "valid_email_1": "email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'",
    "valid_is_active_2": "is_active IS NOT NULL"
})
def create_customer_data() -> DataFrame:
    """
    Creates DLT table from CSV source with data quality expectations.

    Source: test_data.csv
    Columns: 8
    Boolean columns with cleansing: 2
    Expectations: 3
    """
    # Read source data
    df = spark.read.csv("path/to/test_data.csv", header=True)

    # Boolean Cleansing using helper function
    df = flag_x(df, "IS_ACTIVE_FLAG")
    df = flag_x(df, "CONSENT_FLAG")

    # Column transformations
    df = df \
        .withColumnRenamed("CUSTOMER_ID", "customer_id") \
        .withColumnRenamed("FIRST_NAME", "first_name") \
        .withColumnRenamed("LAST_NAME", "last_name") \
        .withColumnRenamed("EMAIL", "email") \
        .withColumnRenamed("IS_ACTIVE_FLAG", "is_active") \
        .withColumnRenamed("CONSENT_FLAG", "consent_given") \
        .withColumnRenamed("REGISTRATION_DATE", "registration_date") \
        .withColumnRenamed("TOTAL_ORDERS", "total_orders") \
        .withColumn("customer_id", F.col("customer_id").cast("int")) \
        .withColumn("total_orders", F.col("total_orders").cast("int"))

    return df
```

---

## Technische Details

### Git-Integration Flow

```
User Input: Repository URL + Branch
    ↓
GitHub API Request: /repos/{owner}/{repo}/contents/cleansing_templates
    ↓
Parse File List (filter *.py)
    ↓
Fetch each file via download_url
    ↓
Store in gitConfig.templates['filename'] = content
    ↓
Save to localStorage.git_templates
    ↓
Display loaded templates in UI
```

### Boolean-Cleansing Integration

```
User marks columns as Boolean
    ↓
Code Generation triggered
    ↓
Check if gitConfig.templates['boolean_cleansing'] exists
    ↓
YES → Add Helper Functions Section
    ↓
For each boolean column:
    - Apply: df = flag_x(df, "ORIGINAL_NAME")
    ↓
Continue with renaming and type casting
```

### localStorage Persistenz

Folgende Daten werden automatisch gespeichert:

```javascript
localStorage.git_repo_url      // Repository URL
localStorage.git_branch        // Branch name
localStorage.git_templates     // JSON mit allen Templates
```

Beim nächsten Besuch werden diese automatisch geladen.

---

## Vorteile gegenüber separaten Generatoren

| Feature | Separate Generatoren | Unified Generator |
|---------|---------------------|-------------------|
| **Design** | Unterschiedlich | Einheitlich professionell |
| **Git-Integration** | Nur in 2 Generatoren | Robust implementiert |
| **Boolean-Cleansing** | Teilweise | Vollständig mit Helper-Functions |
| **Pattern-Helper** | Nur in 1 Generator | Integriert |
| **Code-Struktur** | Inkonsistent | Einheitliche Sections |
| **Wartbarkeit** | 4 verschiedene Dateien | Eine zentrale Datei |

---

## Beispiel-Workflow

### Szenario: Kundendaten mit Boolean-Flags

**1. Vorbereitung (einmalig)**
   - GitHub-Repo erstellen: `my-company/data-templates`
   - Ordner `cleansing_templates/` anlegen
   - `boolean_cleansing.py` mit `flag_x` Funktion hinzufügen

**2. Generator öffnen**
   - `databricks-dlt-generator-unified.html` im Browser öffnen

**3. Git konfigurieren**
   - URL eingeben: `https://github.com/my-company/data-templates`
   - Branch: `main`
   - "Templates laden" → ✓ 1 Template geladen

**4. CSV hochladen**
   - `test_data.csv` auswählen
   - Automatischer Wechsel zu Tab 2

**5. Konfigurieren**
   - Tabellenname: `customer_data`
   - IS_ACTIVE_FLAG → is_active + ☑ Boolean
   - CONSENT_FLAG → consent_given + ☑ Boolean
   - Expectations hinzufügen (NOT NULL, E-Mail-Pattern)

**6. Generieren**
   - Tab 3 öffnen
   - "Python-Code generieren"
   - Code enthält nun automatisch die `flag_x` Helper-Funktion
   - Download als `customer_data.py`

---

## Browser-Kompatibilität

- ✅ Chrome/Edge (ab Version 90)
- ✅ Firefox (ab Version 88)
- ✅ Safari (ab Version 14)

**Anforderungen:**
- JavaScript aktiviert
- localStorage verfügbar
- CORS-Zugriff auf GitHub API (automatisch gegeben)

---

## Fehlerbehebung

### Git-Templates laden nicht

**Problem:** Fehler beim Laden der Templates

**Lösungen:**
1. Repository muss **öffentlich** sein (private Repos benötigen Token)
2. Ordner muss exakt `cleansing_templates` heißen (case-sensitive)
3. Branch-Name prüfen (default: `main`, nicht `master`)
4. GitHub API Rate-Limit (60 Requests/Stunde ohne Token)

### Boolean-Cleansing wird nicht eingefügt

**Problem:** Trotz markierter Boolean-Spalten keine `flag_x` Funktion

**Lösungen:**
1. Template muss exakt `boolean_cleansing.py` heißen
2. "Templates laden" Button gedrückt?
3. Browser-Console öffnen und nach Fehlern suchen

---

## Weiterentwicklung

### Mögliche Erweiterungen

1. **Private Repository Support**
   - GitHub Personal Access Token Integration
   - OAuth Flow

2. **Weitere Templates**
   - Email-Validierung
   - Datum-Formatierung
   - String-Normalisierung
   - Null-Handling

3. **Template-Marketplace**
   - Vorgefertigte Templates aus Community
   - One-Click-Installation

4. **Code-Preview**
   - Live-Update während Konfiguration
   - Syntax-Highlighting

5. **Export-Optionen**
   - Notebook-Format (.ipynb)
   - SQL-Alternative (Delta Lake SQL)
   - Terraform-Definition

---

## Lizenz

Dieses Projekt verwendet die gleiche Lizenz wie das Haupt-Repository.

---

## Support

Bei Fragen oder Problemen:
1. Prüfen Sie die Fehlerbehebung-Sektion
2. Öffnen Sie ein Issue im Repository
3. Kontaktieren Sie das Data Engineering Team

---

**Erstellt:** Oktober 2025
**Version:** 1.0 (Unified Edition)
**Letzte Aktualisierung:** 2025-10-31

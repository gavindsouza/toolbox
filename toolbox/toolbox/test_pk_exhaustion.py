# Copyright (c) 2023, Gavin D'souza and Contributors
# See license.txt
#
# TDD tests for Feature 8: Primary Key Exhaustion Monitoring

import unittest
from unittest.mock import MagicMock, patch


class TestPKMaxValues(unittest.TestCase):
    """Test that we correctly determine max values for different integer types."""

    def test_int_signed_max(self):
        from toolbox.toolbox.doctype.mariadb_index.pk_exhaustion import get_max_value_for_type

        self.assertEqual(get_max_value_for_type("int"), 2_147_483_647)

    def test_int_unsigned_max(self):
        from toolbox.toolbox.doctype.mariadb_index.pk_exhaustion import get_max_value_for_type

        self.assertEqual(get_max_value_for_type("int unsigned"), 4_294_967_295)

    def test_bigint_signed_max(self):
        from toolbox.toolbox.doctype.mariadb_index.pk_exhaustion import get_max_value_for_type

        self.assertEqual(get_max_value_for_type("bigint"), 9_223_372_036_854_775_807)

    def test_bigint_unsigned_max(self):
        from toolbox.toolbox.doctype.mariadb_index.pk_exhaustion import get_max_value_for_type

        self.assertEqual(get_max_value_for_type("bigint unsigned"), 18_446_744_073_709_551_615)

    def test_smallint_signed_max(self):
        from toolbox.toolbox.doctype.mariadb_index.pk_exhaustion import get_max_value_for_type

        self.assertEqual(get_max_value_for_type("smallint"), 32_767)

    def test_tinyint_signed_max(self):
        from toolbox.toolbox.doctype.mariadb_index.pk_exhaustion import get_max_value_for_type

        self.assertEqual(get_max_value_for_type("tinyint"), 127)

    def test_unknown_type_returns_none(self):
        from toolbox.toolbox.doctype.mariadb_index.pk_exhaustion import get_max_value_for_type

        self.assertIsNone(get_max_value_for_type("varchar"))
        self.assertIsNone(get_max_value_for_type("text"))


class TestPKUsageCalculation(unittest.TestCase):
    """Test percentage calculation for PK exhaustion."""

    def test_percentage_calculation(self):
        from toolbox.toolbox.doctype.mariadb_index.pk_exhaustion import calculate_pk_usage

        result = calculate_pk_usage(auto_increment=1_000_000, max_value=2_147_483_647)
        self.assertAlmostEqual(result, 0.047, places=2)

    def test_high_usage(self):
        from toolbox.toolbox.doctype.mariadb_index.pk_exhaustion import calculate_pk_usage

        result = calculate_pk_usage(auto_increment=1_900_000_000, max_value=2_147_483_647)
        self.assertGreater(result, 80.0)

    def test_zero_auto_increment(self):
        from toolbox.toolbox.doctype.mariadb_index.pk_exhaustion import calculate_pk_usage

        result = calculate_pk_usage(auto_increment=0, max_value=2_147_483_647)
        self.assertEqual(result, 0.0)

    def test_none_auto_increment(self):
        from toolbox.toolbox.doctype.mariadb_index.pk_exhaustion import calculate_pk_usage

        result = calculate_pk_usage(auto_increment=None, max_value=2_147_483_647)
        self.assertIsNone(result)


class TestPKSeverityClassification(unittest.TestCase):
    """Test severity levels based on usage percentage."""

    def test_green_under_50(self):
        from toolbox.toolbox.doctype.mariadb_index.pk_exhaustion import classify_pk_severity

        self.assertEqual(classify_pk_severity(30.0), "green")
        self.assertEqual(classify_pk_severity(0.0), "green")
        self.assertEqual(classify_pk_severity(49.9), "green")

    def test_yellow_50_to_80(self):
        from toolbox.toolbox.doctype.mariadb_index.pk_exhaustion import classify_pk_severity

        self.assertEqual(classify_pk_severity(50.0), "yellow")
        self.assertEqual(classify_pk_severity(70.0), "yellow")
        self.assertEqual(classify_pk_severity(79.9), "yellow")

    def test_red_over_80(self):
        from toolbox.toolbox.doctype.mariadb_index.pk_exhaustion import classify_pk_severity

        self.assertEqual(classify_pk_severity(80.0), "red")
        self.assertEqual(classify_pk_severity(90.0), "red")
        self.assertEqual(classify_pk_severity(100.0), "red")

    def test_none_returns_none(self):
        from toolbox.toolbox.doctype.mariadb_index.pk_exhaustion import classify_pk_severity

        self.assertIsNone(classify_pk_severity(None))


class TestGetPKExhaustionReport(unittest.TestCase):
    """Test the full report generation."""

    @patch("toolbox.toolbox.doctype.mariadb_index.pk_exhaustion.frappe")
    def test_report_structure(self, mock_frappe):
        from toolbox.toolbox.doctype.mariadb_index.pk_exhaustion import get_pk_exhaustion_report

        mock_frappe.db.sql.side_effect = [
            # First call: get tables with auto-increment
            [
                {"TABLE_NAME": "tabUser", "AUTO_INCREMENT": 1000, "COLUMN_TYPE": "int(11)"},
                {"TABLE_NAME": "tabActivity Log", "AUTO_INCREMENT": 2_000_000_000, "COLUMN_TYPE": "int(11)"},
            ],
        ]

        report = get_pk_exhaustion_report()

        self.assertIsInstance(report, list)
        self.assertEqual(len(report), 2)
        for entry in report:
            self.assertIn("table_name", entry)
            self.assertIn("auto_increment", entry)
            self.assertIn("max_value", entry)
            self.assertIn("usage_percent", entry)
            self.assertIn("severity", entry)

    @patch("toolbox.toolbox.doctype.mariadb_index.pk_exhaustion.frappe")
    def test_sorts_by_usage_desc(self, mock_frappe):
        from toolbox.toolbox.doctype.mariadb_index.pk_exhaustion import get_pk_exhaustion_report

        mock_frappe.db.sql.return_value = [
            {"TABLE_NAME": "tabLow", "AUTO_INCREMENT": 100, "COLUMN_TYPE": "int(11)"},
            {"TABLE_NAME": "tabHigh", "AUTO_INCREMENT": 2_000_000_000, "COLUMN_TYPE": "int(11)"},
        ]

        report = get_pk_exhaustion_report()

        self.assertEqual(report[0]["table_name"], "tabHigh")
        self.assertEqual(report[1]["table_name"], "tabLow")

    @patch("toolbox.toolbox.doctype.mariadb_index.pk_exhaustion.frappe")
    def test_filters_by_threshold(self, mock_frappe):
        from toolbox.toolbox.doctype.mariadb_index.pk_exhaustion import get_pk_exhaustion_report

        mock_frappe.db.sql.return_value = [
            {"TABLE_NAME": "tabLow", "AUTO_INCREMENT": 100, "COLUMN_TYPE": "int(11)"},
            {"TABLE_NAME": "tabHigh", "AUTO_INCREMENT": 2_000_000_000, "COLUMN_TYPE": "int(11)"},
        ]

        report = get_pk_exhaustion_report(min_usage_percent=50.0)

        self.assertEqual(len(report), 1)
        self.assertEqual(report[0]["table_name"], "tabHigh")


class TestParseColumnType(unittest.TestCase):
    """Test parsing COLUMN_TYPE strings from INFORMATION_SCHEMA."""

    def test_int_with_display_width(self):
        from toolbox.toolbox.doctype.mariadb_index.pk_exhaustion import parse_column_type

        self.assertEqual(parse_column_type("int(11)"), "int")

    def test_int_unsigned_with_display_width(self):
        from toolbox.toolbox.doctype.mariadb_index.pk_exhaustion import parse_column_type

        self.assertEqual(parse_column_type("int(11) unsigned"), "int unsigned")

    def test_bigint(self):
        from toolbox.toolbox.doctype.mariadb_index.pk_exhaustion import parse_column_type

        self.assertEqual(parse_column_type("bigint(20)"), "bigint")

    def test_bigint_unsigned(self):
        from toolbox.toolbox.doctype.mariadb_index.pk_exhaustion import parse_column_type

        self.assertEqual(parse_column_type("bigint(20) unsigned"), "bigint unsigned")

    def test_smallint(self):
        from toolbox.toolbox.doctype.mariadb_index.pk_exhaustion import parse_column_type

        self.assertEqual(parse_column_type("smallint(6)"), "smallint")

    def test_plain_int(self):
        from toolbox.toolbox.doctype.mariadb_index.pk_exhaustion import parse_column_type

        self.assertEqual(parse_column_type("int"), "int")


if __name__ == "__main__":
    unittest.main()

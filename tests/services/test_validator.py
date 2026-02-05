import pytest

from app.services.validator import PanelValidation, TestValidation
from datetime import datetime, timezone


class TestValidationParsing:
    @pytest.fixture
    def test_validation(self):
        return TestValidation()

    @pytest.mark.parametrize(
        ("result", "expected_comparator", "expected_value_num"),
        [
            ("> 5", ">", 5.0),
            ("<=1425.2", "<=", 1425.2),
            ("= 0.2", "=", 0.2),
            ("5", None, 5.0),
            ("Not Detected", None, None),
        ],
    )

    # Parsing and validation of "result" field
    def test_result_parsing(
        self,
        test_validation,
        make_row,
        result,
        expected_comparator,
        expected_value_num,
    ):
        row = make_row(result=result)
        payload, errors = test_validation.build_test_payload(row, row_number=3)

        assert errors == []
        assert payload is not None
        assert payload["result_comparator"] == expected_comparator
        assert payload["result_value_num"] == expected_value_num
        assert payload["result_raw"] == result

    def test_negative_numeric_rejected(self, test_validation, make_row):
        row = make_row(result="-4")
        payload, errors = test_validation.build_test_payload(row, row_number=6)

        assert payload is None
        assert len(errors) == 1
        assert errors[0].field == "result"

    # Error when required field missing, ok when an optional field missing
    def test_required_field_missing_error(self, test_validation, make_row):
        row = make_row(result="")

        payload, errors = test_validation.build_test_payload(row, row_number=5)

        assert payload is None
        assert len(errors) == 1
        assert errors[0].field == "result"
        assert errors[0].message == "required field missing"

    def test_optional_field_missing_ok(self, test_validation, make_row):
        row = make_row(reference_range_low="")

        payload, errors = test_validation.build_test_payload(row, row_number=1)

        assert errors == []
        assert payload is not None
        assert payload["ref_low_raw"] is None


class TestPanelValidationParsing:
    @pytest.fixture
    def panel_validation(self):
        return PanelValidation()

    def test_patient_id_format_ok(self, panel_validation, make_row):
        row = make_row(patient_id="PAT-eafeb37f-fd58-4763-bb13-b7299fb488ef")
        payload, errors = panel_validation.build_panel_payload(
            row, row_number=3
        )

        assert errors == []
        assert payload is not None
        assert (
            payload["patient_id"] == "PAT-eafeb37f-fd58-4763-bb13-b7299fb488ef"
        )

    @pytest.mark.parametrize(
        ("patient_id", "part_message"),
        [
            ("eafeb37f-fd58-4763-bb13-b7299fb488ef", "must start with"),
            ("PAT-007", "id format must be"),
        ],
    )
    def test_patient_id_incorrect_format_error(
        self, panel_validation, make_row, patient_id, part_message
    ):
        row = make_row(patient_id=patient_id)
        payload, errors = panel_validation.build_panel_payload(
            row, row_number=8
        )

        assert payload is None
        assert len(errors) == 1
        assert part_message in errors[0].message

    @pytest.mark.parametrize(
        "collection_timestamp",
        ["2024-02-04T14:37:00+00:00", "2024-02-04 14:37:00"],
    )
    def test_collection_timestamp_valid_ok(
        self, panel_validation, make_row, collection_timestamp
    ):
        row = make_row(collection_timestamp=collection_timestamp)

        payload, errors = panel_validation.build_panel_payload(
            row, row_number=9
        )
        expected = datetime.fromisoformat(collection_timestamp)
        if expected.tzinfo is None:
            expected = expected.replace(tzinfo=timezone.utc)

        assert payload is not None
        assert errors == []
        assert payload["collection_timestamp"] == expected

    @pytest.mark.parametrize(
        ("collection_timestamp", "part_message"),
        [
            ("2036-01-28T16:05:33+00:00", "cannot be in future"),
            ("28 January 2026", "must be ISO 8601 datetime"),
        ],
    )
    def test_collection_timestamp_invalid_error(
        self, panel_validation, make_row, collection_timestamp, part_message
    ):
        row = make_row(collection_timestamp=collection_timestamp)

        payload, errors = panel_validation.build_panel_payload(
            row, row_number=10
        )

        assert payload is None
        assert len(errors) == 1
        assert part_message in errors[0].message
        assert errors[0].field == "collection_timestamp"


class TestPanelValidationGrouping:
    @pytest.fixture
    def panel_validation(self):
        return PanelValidation()

    def test_determine_panels_groups_two_rows_same_panel(
        self, panel_validation, rows_same_panel_two_tests
    ):
        groups, errors = panel_validation.determine_panels(
            rows_same_panel_two_tests
        )

        assert errors == []
        assert len(groups) == 1

        (key, group) = next(iter(groups.items()))
        assert isinstance(key, tuple)
        assert set(group.keys()) == {"panel_payload", "panel_rows"}

        assert group["panel_payload"] is not None
        assert group["panel_payload"]["panel_code"] == "LIPID"

        # Row numbers should be CSV row numbers (1-based), preserved in panel_rows
        assert group["panel_rows"][0][0] == 1
        assert group["panel_rows"][1][0] == 2

        # Stored original row dicts
        assert group["panel_rows"][0][1]["test_code"] == "TC"
        assert group["panel_rows"][1][1]["test_code"] == "HDL"

    def test_determine_panels_splits_into_two_groups(
        self, panel_validation, rows_two_panels
    ):
        groups, errors = panel_validation.determine_panels(rows_two_panels)

        assert errors == []
        assert len(groups) == 2

        panel_codes = {k[0] for k in groups.keys()}
        assert panel_codes == {"BMP", "LIPID"}

    def test_determine_panels_group_key_contains_normalized_timestamp(
        self, panel_validation, make_row
    ):
        # Naive timestamp should be normalized to UTC in payload, and thus in the group key
        rows = [
            make_row(
                collection_timestamp="2024-02-04 14:37:00", test_code="TC"
            ),
            make_row(
                collection_timestamp="2024-02-04 14:37:00", test_code="HDL"
            ),
        ]

        groups, errors = panel_validation.determine_panels(rows)

        assert errors == []
        assert len(groups) == 1

        (panel_code, sample_id, ts) = next(iter(groups.keys()))
        assert panel_code == "LIPID"
        assert isinstance(ts, datetime)
        assert ts.tzinfo is timezone.utc

    def test_determine_panels_sample_id_blank_groups_as_none(
        self, panel_validation, make_row
    ):
        rows = [
            make_row(sample_id="", test_code="TC"),
            make_row(sample_id="", test_code="HDL"),
        ]

        groups, errors = panel_validation.determine_panels(rows)

        assert errors == []
        assert len(groups) == 1

        (panel_code, sample_id, _ts) = next(iter(groups.keys()))
        assert panel_code == "LIPID"
        assert sample_id is None

    def test_determine_panels_inconsistent_patient_id_in_group_errors_and_skips_row(
        self, panel_validation, make_row
    ):
        # Same group key (panel_code/sample_id/timestamp), but different patient_id
        rows = [
            make_row(
                patient_id="PAT-eafeb37f-fd58-4763-bb13-b7299fb488ef",
                test_code="TC",
            ),
            make_row(
                patient_id="PAT-eafeb37f-fd58-4763-bb13-b7299fb488e1",
                test_code="HDL",
            ),
        ]

        groups, errors = panel_validation.determine_panels(rows)

        assert len(errors) == 1
        assert errors[0].field == "patient_id"
        assert errors[0].row_number == 2
        assert "must be consistent" in errors[0].message

        # The second row should not be added to the group due to inconsistency
        assert len(groups) == 1
        group = next(iter(groups.values()))
        assert len(group["panel_rows"]) == 1
        assert group["panel_rows"][0][0] == 1
        assert group["panel_rows"][0][1]["test_code"] == "TC"

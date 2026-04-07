import re
import json


def parse_input_rows(input_text: str) -> set[int]:
    rows = set()
    for line in input_text.strip().splitlines():
        m = re.match(r"\s*(\d+)\s*:", line)
        if m:
            rows.add(int(m.group(1)))
    return rows


def parse_output_rows(output_json_text: str) -> set[int]:
    data = json.loads(output_json_text)
    return {
        err["row_number"]
        for err in data.get("validation_errors", [])
        if "row_number" in err
    }


def compare_rows(
    input_text: str, output_json_text: str
) -> dict[str, set[int]]:
    expected = parse_input_rows(input_text)
    actual = parse_output_rows(output_json_text)

    return {
        "expected_rows": expected,
        "actual_rows": actual,
        "missing_in_output": expected - actual,
        "unexpected_in_output": actual - expected,
        "matched_rows": expected & actual,
    }


# input_text = """\
# 148: field missing panel_code, field missing test_code
# 598: patient_id differs from the rest by 1 digit
# 5494: result is negative"
# """

# for invalid_large_01.csv
# def get_full_input_text(input_text_partial):
#     input_text = ""
#     for r in range(1, 148):
#         input_text += (
#             f"{r}" + ": field missing panel_code, field missing test_code\n"
#         )

#     input_text += input_text_partial
#     return input_text


# input_text = get_full_input_text(input_text_partial)
# print(input_text)


# for invalid_large_02.csv
# input_text = ""

# for r in range(1, 11):
#     input_text += f"{r}" + " field missing patient_id\n"

# for end_r in range(10801 - 10, 10801):
#     input_text += f"{end_r}" + " field missing patient_id\n"

# print(input_text)

input_text = """"""

output_json_text = """\
{"validation_errors": [{"field": "collection_timestamp", "message": "must be ISO 8601 datetime", "row_number": 1}, {"field": "collection_timestamp", "message": "must be ISO 8601 datetime", "row_number": 2}, {"field": "collection_timestamp", "message": "must be ISO 8601 datetime", "row_number": 3}, {"field": "collection_timestamp", "message": "must be ISO 8601 datetime", "row_number": 4}, {"field": "collection_timestamp", "message": "must be ISO 8601 datetime", "row_number": 5}, {"field": "collection_timestamp", "message": "must be ISO 8601 datetime", "row_number": 6}, {"field": "collection_timestamp", "message": "must be ISO 8601 datetime", "row_number": 7}, {"field": "collection_timestamp", "message": "must be ISO 8601 datetime", "row_number": 8}, {"field": "collection_timestamp", "message": "must be ISO 8601 datetime", "row_number": 9}, {"field": "collection_timestamp", "message": "must be ISO 8601 datetime", "row_number": 10}, {"field": "patient_id", "message": "required field missing", "row_number": 10791}, {"field": "patient_id", "message": "required field missing", "row_number": 10792}, {"field": "patient_id", "message": "required field missing", "row_number": 10793}, {"field": "patient_id", "message": "required field missing", "row_number": 10794}, {"field": "patient_id", "message": "required field missing", "row_number": 10795}, {"field": "patient_id", "message": "required field missing", "row_number": 10796}, {"field": "patient_id", "message": "required field missing", "row_number": 10797}, {"field": "patient_id", "message": "required field missing", "row_number": 10798}, {"field": "patient_id", "message": "required field missing", "row_number": 10799}, {"field": "patient_id", "message": "required field missing", "row_number": 10800}]}
"""

result = compare_rows(input_text, output_json_text)
for k, v in result.items():
    print(f"{k}: {sorted(v)}")

print()

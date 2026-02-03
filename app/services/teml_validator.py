ds = {"rttt": [1000, 10001], "dfdfgbiufbhd": [9845768, 1]}
for value in ds.values():
    print(value[1])


# import re


# def get_comparator(result_raw):
#     comparator_pattern = "^(>=|<=|[>=<])"
#     # match = match.group(0)
#     # end_index = match.end()
#     #     # Get the remaining part of the string after the match
#     #     remaining_part = text[end_index:]
#     match = re.search(comparator_pattern, result_raw)
#     result_value_num = None
#     result_comparator = None
#     if match:
#         result_comparator = match.group(0)
#         end_index = match.end()
#         result_without_comparator = result_raw[end_index:]
#         if result_without_comparator != "":
#             try:
#                 result_value_num = float(result_without_comparator)
#                 print(
#                     f"Comparator is {result_comparator}. Num result is {result_value_num}"
#                 )
#             except ValueError as e:
#                 print(
#                     f"Comparator is {result_comparator}.Result is not numeric "
#                 )
#         else:
#             print(
#                 f"Comparator is {result_comparator}. Numeric Result is not found"
#             )
#     else:
#         try:
#             result_value_num = float(result_raw)
#             print(f"No comparator. result_value_num is {result_value_num}")
#         except ValueError:
#             print("Result is not numeric")


# a = ">=100"
# b = ">=1000!"
# get_comparator(b)


# text = ">5.2"
# # Pattern: ([><=]+) captures operators, ([0-9.]+) captures numbers/decimal
# match = re.search(r"([><=]+)([0-9.]+)", text)

# if match:
#     comparator = match.group(1) # ">"
#     number = match.group(2)     # "5.2"
#     print(f"Comparator: {comparator}, Number: {number}")

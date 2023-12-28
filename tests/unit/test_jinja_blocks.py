from dbt_meshify.storage.jinja_blocks import JinjaBlock

string = """\


{% docs customer_id %}
The unique key for each customer.
{% enddocs %}
"""

multiple_blocks = """\


{% docs customer_id %}
The unique key for each customer.
{% enddocs %}

{% docs potato_name %}
The name of the customer's favorite potato dish.
{% enddocs %}
"""


class TestJinjaBlock:
    def test_from_file_detects_block_range(self):
        range = JinjaBlock.find_block_range(string, "docs", "customer_id")
        assert range == (2, 4)

    def test_from_file_extracts_content(self):
        content = JinjaBlock.isolate_content_from_line_range(string, 2, 4)
        assert content == "The unique key for each customer."

    def test_from_file_detects_block_range_in_multiple_blocks(self):
        range = JinjaBlock.find_block_range(multiple_blocks, "docs", "potato_name")
        assert range == (6, 8)

    def test_from_file_extracts_content_in_files_with_multiple_blocks(self):
        content = JinjaBlock.isolate_content_from_line_range(multiple_blocks, 6, 8)
        assert content == "The name of the customer's favorite potato dish."

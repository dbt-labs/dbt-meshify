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
        assert range == (2, 72)

    def test_from_file_extracts_content(self):
        content = JinjaBlock.isolate_content(string, 2, 72)
        assert (
            content == "{% docs customer_id %}\nThe unique key for each customer.\n{% enddocs %}"
        )

    def test_from_file_detects_block_range_in_multiple_blocks(self):
        range = JinjaBlock.find_block_range(multiple_blocks, "docs", "potato_name")
        assert range == (74, 159)

    def test_from_file_extracts_content_in_files_with_multiple_blocks(self):
        content = JinjaBlock.isolate_content(multiple_blocks, 74, 159)
        assert (
            content
            == "{% docs potato_name %}\nThe name of the customer's favorite potato dish.\n{% enddocs %}"
        )

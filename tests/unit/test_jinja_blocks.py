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

no_leading_space = """\


{%docs customer_id %}
The unique key for each customer.
{% enddocs %}
"""

no_trailing_space = """\


{% docs customer_id%}
The unique key for each customer.
{% enddocs %}
"""

no_spaces = """\


{%docs customer_id%}
The unique key for each customer.
{% enddocs %}
"""

no_spaces_end_docs = """\


{%docs customer_id%}
The unique key for each customer.
{%enddocs%}
"""

special_character = """\


{% docs cust-omer_id %}
The unique key for each customer.
{% enddocs %}
"""

simple_macro = """\


{% macro test_macro(name) %}
  {{ name }}
{% endmacro %}
"""

simple_macro_no_spaces = """\


{%macro test_macro(name)%}
  {{ name }}
{%endmacro%}
"""

simple_macro_space_to_args = """\


{% macro test_macro (name) %}
  {{ name }}
{% endmacro %}
"""

simple_macro_string_defaults = """\


{% macro test_macro(name='dave') %}
  {{ name }}
{% endmacro %}
"""

simple_macro_string_defaults_double_quotes = """\


{% macro test_macro(name="dave") %}
  {{ name }}
{% endmacro %}
"""

simple_macro_int_defaults = """\


{% macro test_macro(num=8) %}
  {{ num }}
{% endmacro %}
"""


class TestJinjaBlock:
    def test_from_file_detects_block_range(self):
        range = JinjaBlock.find_block_range(string, "docs", "customer_id")
        assert range == (2, 72)

    def test_from_file_detects_block_range_no_leading_space(self):
        range = JinjaBlock.find_block_range(no_leading_space, "docs", "customer_id")
        assert range == (2, 71)

    def test_from_file_detects_block_range_no_trailing_space(self):
        range = JinjaBlock.find_block_range(no_trailing_space, "docs", "customer_id")
        assert range == (2, 71)

    def test_from_file_detects_block_range_no_spaces(self):
        range = JinjaBlock.find_block_range(no_spaces, "docs", "customer_id")
        assert range == (2, 70)

    def test_from_file_detects_block_range_no_spaces_end_docs(self):
        range = JinjaBlock.find_block_range(no_spaces_end_docs, "docs", "customer_id")
        assert range == (2, 68)

    def test_from_file_detects_block_range_special_character(self):
        range = JinjaBlock.find_block_range(special_character, "docs", "cust-omer_id")
        assert range == (2, 73)

    def test_from_file_detects_block_range_simple_macro(self):
        range = JinjaBlock.find_block_range(simple_macro, "macro", "test_macro")
        assert range == (2, 58)

    def test_from_file_detects_block_range_simple_macro_no_spaces(self):
        range = JinjaBlock.find_block_range(simple_macro_no_spaces, "macro", "test_macro")
        assert range == (2, 54)

    def test_from_file_detects_block_range_simple_macro_space_to_args(self):
        range = JinjaBlock.find_block_range(simple_macro_space_to_args, "macro", "test_macro")
        assert range == (2, 59)

    def test_from_file_detects_block_range_simple_macro_string_defaults(self):
        range = JinjaBlock.find_block_range(simple_macro_string_defaults, "macro", "test_macro")
        assert range == (2, 65)

    def test_from_file_detects_block_range_simple_macro_string_defaults_double_quotes(self):
        range = JinjaBlock.find_block_range(
            simple_macro_string_defaults_double_quotes, "macro", "test_macro"
        )
        assert range == (2, 65)

    def test_from_file_detects_block_range_simple_macro_int_defaults(self):
        range = JinjaBlock.find_block_range(simple_macro_int_defaults, "macro", "test_macro")
        assert range == (2, 58)

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

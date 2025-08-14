import json

from .models import ComparableTransform
from .display import ComparisonDisplay


class TransformComparator:
    """
    Orchestrates the comparison between two transforms using dedicated models.
    """

    def __init__(self, transform_a_data: dict, transform_b_data: dict,
                 desc_a: str, desc_b: str):
        self.transform_a = ComparableTransform(transform_a_data, desc_a)
        self.transform_b = ComparableTransform(transform_b_data, desc_b)
        self.results = {}

    def run(self):
        """Executes all comparison steps and stores the results."""
        self.results['properties'] = self._compare_main_properties()
        self.results['settings'] = self._compare_settings()
        self.results['output_columns'] = self._compare_output_columns()
        return self.results

    def display(self):
        """Passes the results to the display class for rendering."""
        displayer = ComparisonDisplay(self.transform_a, self.transform_b, self.results)
        displayer.render()

    def _compare_main_properties(self) -> list:
        """Compares high-level properties."""
        comparisons = [
            {'key': 'type', 'val_a': self.transform_a.type, 'val_b': self.transform_b.type},
            {'key': 'sql', 'val_a': self.transform_a.sql, 'val_b': self.transform_b.sql}
        ]
        return comparisons

    def _compare_settings(self) -> dict:
        """Compares all keys within the 'settings' property."""
        settings_a = self.transform_a.settings
        settings_b = self.transform_b.settings
        all_keys = sorted(list(set(settings_a.keys()) | set(settings_b.keys())))

        diffs = []
        for key in all_keys:
            val_a = settings_a.get(key)
            val_b = settings_b.get(key)
            if val_a != val_b:
                # Pretty-print JSON-like structures for better readability
                val_a_str = json.dumps(val_a, indent=2) if isinstance(val_a, (dict, list)) else str(val_a)
                val_b_str = json.dumps(val_b, indent=2) if isinstance(val_b, (dict, list)) else str(val_b)
                diffs.append({'key': key, 'val_a': val_a_str, 'val_b': val_b_str})
        return {'diffs': diffs}

    def _compare_output_columns(self) -> dict:
        """Compares output columns and identifies attribute differences."""
        cols_a = self.transform_a.output_columns
        cols_b = self.transform_b.output_columns

        names_a = set(cols_a.keys())
        names_b = set(cols_b.keys())

        results = {
            'only_in_b': sorted(list(names_b - names_a)),
            'only_in_a': sorted(list(names_a - names_b)),
            'modified': []
        }

        for name in sorted(list(names_a.intersection(names_b))):
            col_a = cols_a[name]
            col_b = cols_b[name]

            if col_a != col_b:
                # Find just the exact differences
                attrs_a = col_a.as_comparable_dict()
                attrs_b = col_b.as_comparable_dict()

                key_diffs = []
                all_keys = sorted(list(set(attrs_a.keys()) | set(attrs_b.keys())))

                for key in all_keys:
                    val_a = attrs_a.get(key)
                    val_b = attrs_b.get(key)
                    if val_a != val_b:
                        key_diffs.append({
                            "attribute": key,
                            "value_a": json.dumps(val_a, indent=2) if isinstance(val_a, (dict, list)) else str(val_a),
                            "value_b": json.dumps(val_b, indent=2) if isinstance(val_b, (dict, list)) else str(val_b)
                        })

                if key_diffs:
                    results['modified'].append({'name': name, 'diffs': key_diffs})

        return results

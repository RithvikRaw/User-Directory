import os


class SavedQueries:
    """Class to retrieve saved queries from the sql folder."""

    def __init__(self):
        files = os.listdir(os.path.dirname(os.path.abspath(__file__)))
        self.queries = {
            f[:-4]: open(
                os.path.join(os.path.dirname(os.path.abspath(__file__)), f)
            ).read()
            for f in files
            if f.endswith(".sql")
        }

    def get_query(self, query_name) -> str:
        """Get a saved query by name."""
        if query_name not in self.queries:
            raise ValueError(f"Query {query_name} not found.")
        return self.queries.get(query_name)

    def get_all_queries(self) -> list[str]:
        """Get all saved queries' names in a list."""
        return list(self.queries.keys())


SAVED_QUERIES = SavedQueries()


def get_query(query_name: str) -> str:
    """Get a saved query by name."""
    return SAVED_QUERIES.get_query(query_name)


def get_all_queries() -> list[str]:
    """Get all saved queries' names in a list."""
    return SAVED_QUERIES.get_all_queries()

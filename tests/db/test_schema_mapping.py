from juneau.db.schemamapping import SchemaMapping


def test_jaccard_similarity():
    col1 = [1, 2, 3, 4, 5, 6]
    col2 = [1, 2, 0, 0, 0, 0]
    assert SchemaMapping.jaccard_similarity(col1, col2) == 2 / 7
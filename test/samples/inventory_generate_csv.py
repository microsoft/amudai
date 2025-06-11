import csv
import random
import sys


def create_pseudo_word_vocabulary():
    """Creates a vocabulary of 500 pseudo-words in lorem ipsum style."""
    prefixes = ["ad", "ante", "con", "de", "ex", "in",
                "inter", "pre", "pro", "re", "sub", "trans", "ultra"]
    roots = ["aqu", "bene", "cred", "dict", "duc", "fac", "graph",
             "loc", "mit", "port", "script", "spect", "ven", "vid"]
    middles = ["am", "er", "est", "il", "in", "is", "it", "or", "um", "us"]
    suffixes = ["able", "ance", "ent", "ful", "ing",
                "ion", "ive", "ly", "ment", "ous", "tion"]

    fragments = ["lorem", "ipsum", "dolor", "amet", "consectetur", "adipiscing", "elit", "sed", "eiusmod", "tempor",
                 "incididunt", "labore", "magna", "aliqua", "enim", "minim", "veniam", "quis", "nostrud",
                 "exercitation", "ullamco", "laboris", "nisi", "aliquip", "commodo", "consequat", "duis",
                 "aute", "irure", "reprehenderit", "voluptate", "velit", "esse", "cillum", "fugiat", "nulla",
                 "pariatur", "excepteur", "sint", "occaecat", "cupidatat", "proident", "sunt", "culpa", "qui",
                 "officia", "deserunt", "mollit", "anim", "laborum"]

    vocabulary = set()

    vocabulary.update(fragments)

    for prefix in prefixes:
        for root in roots:
            for suffix in suffixes:
                if len(prefix + root + suffix) >= 4:
                    vocabulary.add(prefix + root + suffix)

    for root in roots:
        for suffix in suffixes:
            if len(root + suffix) >= 4:
                vocabulary.add(root + suffix)

    for root in roots:
        for middle in middles:
            for suffix in suffixes:
                word = root + middle + suffix
                if 4 <= len(word) <= 16:
                    vocabulary.add(word)

    for i, root1 in enumerate(roots[:20]):
        for root2 in roots[i+1:21]:
            word = root1 + root2
            if 4 <= len(word) <= 16:
                vocabulary.add(word)

    vocab_list = list(vocabulary)

    if len(vocab_list) > 500:
        vocab_list = random.sample(vocab_list, 500)

    while len(vocab_list) < 500:
        base_word = random.choice(vocab_list[:50])  # Use early words as base
        if len(base_word) < 12:
            modified = base_word + \
                random.choice(["us", "um", "is", "es", "or", "ar"])
            if 4 <= len(modified) <= 16 and modified not in vocab_list:
                vocab_list.append(modified)
        else:
            new_word = random.choice(prefixes) + random.choice(middles)
            if 4 <= len(new_word) <= 16 and new_word not in vocab_list:
                vocab_list.append(new_word)

    return vocab_list[:50]


def generate_pseudo_sentence(vocabulary):
    delimiters = [" ", " ", " ", " ", ", ", ". ",
                  ": ", "; "]

    num_words = random.randint(0, 20)

    if num_words == 0:
        return ""

    sentence_parts = []
    for i in range(num_words):
        word = random.choice(vocabulary)
        while len(word) < 4 or len(word) > 16:
            word = random.choice(vocabulary)

        sentence_parts.append(word)

        if i < num_words - 1:
            sentence_parts.append(random.choice(delimiters))

    return "".join(sentence_parts)


def generate_csv_record(record_id, vocabulary):
    num = random.randint(0, 100)
    text = generate_pseudo_sentence(vocabulary)
    return [record_id, num, text]


def print_usage():
    print("Usage: python inventory_generate_csv.py <count> [file_path]")
    print("  count      - Number of records to generate (positive integer)")
    print("  file_path  - Optional output file path. If not provided, outputs to stdout")
    print("")
    print("Each record contains:")
    print("  - record_id: Integer starting at 0, incremented for each record")
    print("  - num: Random integer from 0 to 100")
    print("  - text: Pseudo-sentence with 0-20 lorem ipsum style words")


if __name__ == "__main__":
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print_usage()
        sys.exit(1)

    try:
        count = int(sys.argv[1])
        if count < 0:
            raise ValueError("Count must be non-negative")
    except ValueError as e:
        print(f"Error: Invalid count argument - {e}")
        print_usage()
        sys.exit(1)

    output_file = None
    if len(sys.argv) == 3:
        output_file = sys.argv[2]

    random.seed(0x96d7ef1ea0d6)

    vocabulary = create_pseudo_word_vocabulary()

    # Determine output destination
    if output_file:
        try:
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                for i in range(count):
                    record = generate_csv_record(i, vocabulary)
                    writer.writerow(record)
        except IOError as e:
            print(f"Error: Could not write to file '{output_file}' - {e}")
            sys.exit(1)
    else:
        # Output to stdout
        writer = csv.writer(sys.stdout)
        for i in range(count):
            record = generate_csv_record(i, vocabulary)
            writer.writerow(record)

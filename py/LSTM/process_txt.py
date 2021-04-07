
import collections
import csv

act_delimiter = ":||:"
line_delimiter = "\n"


def _read_words(filepath):
    with open(filepath, "r") as f:
        return f.read().replace(line_delimiter, " ").replace(act_delimiter, " ").split()


def _build_vocab(filepath):
    data = _read_words(filepath)

    counter = collections.Counter(data)
    count_pairs = sorted(counter.items(), key=lambda x: (-x[1], x[0]))

    words, _ = list(zip(*count_pairs))
    word_to_id = dict(zip(words, range(len(words))))

    return word_to_id


def _export_word_id(word_to_id):
    w = csv.writer(open("dict_id_output.csv", "w"))
    for key, val in word_to_id.items():
        w.writerow([key, val])
    pass


def _file_to_word_ids(filepath, word_to_id):
    data = _read_words(filepath)
    return [word_to_id[word] for word in data]


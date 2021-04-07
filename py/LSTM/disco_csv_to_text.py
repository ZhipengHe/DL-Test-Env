
import dask.dataframe as dd

filenames = {}
case_col = "Case ID"
act_col = "Activity"
act_delimiter = ":||:"
line_delimiter = "\n"
EOC = ["EOC"]


def _csv_to_ddf(filepath):
    ddf = dd.read_csv(filepath)
    ddf = ddf.groupby(case_col)[act_col].apply(
        list, meta=(act_col, 'object')).compute().to_frame()
    return ddf


def _add_end_word(filepath):
    ddf = _csv_to_ddf(filepath)
    for index, row in ddf.iterrows():
        row[act_col] = row[act_col] + EOC
    return ddf


def _ddf_to_text(filepath, ddf):
    f = open(filepath, "w")
    for index, row in ddf.iterrows():
        listToStr = act_delimiter.join(map(str, row[act_col])).replace(" ", "")
        f.write(listToStr)
        f.write(line_delimiter)
    f.close()
    pass


def csv_to_txt():
    for filename in filenames:
        print(filename)
        csv_path = '../data/' + filename + '.csv'
        txt_path = '../data/' + filename + '.txt'

        _ddf_to_text(txt_path, _add_end_word(csv_path))
    pass


if __name__ == "__main__":
    csv_to_txt()

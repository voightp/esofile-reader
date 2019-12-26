import os
import shutil


def list_files(root, depth, ext=None, pr_lst=None, excl=None):
    """ Find paths for files using given extension. """
    files = []
    walk(root, depth, files, ext=ext)

    # Sort lists if requested
    if pr_lst:
        files = sort_files(files, pr_lst)

    # Exclude unwanted paths
    if excl:
        files = exclude_files(files, excl)

    return files


def file_names(path_lst):
    names = []
    for path in path_lst:
        full_name = os.path.basename(path)
        name, ext = os.path.splitext(full_name)
        names.append(name)
    return names


def file_dir_name(path):
    return os.path.basename(path)


def parent_dir_name(path):
    pth = os.path.dirname(path)
    return os.path.basename(pth)


def parent_dir(path):
    return os.path.dirname(path)


def grand_parent_dir_name(path):
    pth = os.path.dirname(path)
    pth = os.path.dirname(pth)
    return os.path.basename(pth)


def list_dirs(path):
    dir_names = os.listdir(path)
    return [os.path.join(path, dir_name) for dir_name in dir_names if os.path.join(path, dir_name)]


def sort_files(orig_lst, pr_lst):
    """ Sort files based on first letter of name and priority list. """
    orig_lst.sort()
    new_pth_lst = []
    for pr in pr_lst:
        for pth in orig_lst:
            if os.path.basename(pth)[0].lower() == pr.lower():
                new_pth_lst.append(pth)
    return new_pth_lst


def include_only(orig_lst, f_names_lst):
    """ Return only paths for requested files (request without extension). """
    f_names_lst = [name.lower() for name in f_names_lst]
    return [pth for pth in orig_lst if os.path.splitext(pth)[0].lower() in f_names_lst]


def filter_files(orig_lst, f_names_lst):
    """ Filter and sort list of Eso file objects (not Eso file paths!). """
    if not isinstance(f_names_lst, list):
        f_names_lst = [f_names_lst]

    files = []
    for file_name in f_names_lst:
        for eso_file in orig_lst:
            if eso_file.file_name.lower() == file_name.lower():
                files.append(eso_file)
    return files


def exclude_files(orig_lst, lst_str):
    """ Exclude all files which name contains some of specified strings. """
    if not isinstance(lst_str, list):
        lst_str = [lst_str]

    new_lst = list(orig_lst)
    for str_ in lst_str:
        for pth in orig_lst:
            if str_ in os.path.basename(pth):
                new_lst.remove(pth)
    return new_lst


def walk(root, depth, files, ext=None):
    dirs = []
    for name in os.listdir(root):
        pth = os.path.join(root, name)
        if os.path.isfile(pth):
            if ext:
                if pth.lower().endswith(ext):
                    files.append(pth)
        else:
            dirs.append(pth)
    if depth > 1:
        for pth in dirs:
            walk(pth, depth - 1, files, ext=ext)


def add_version(root, version, depth=2):
    """ Add version number to eso file name. """
    files = list_files(root, depth, ext=("eso", "err", "htm", "idf"))

    for file in files:
        tail, head = os.path.split(file)
        name, ext = os.path.splitext(head)
        new_name = name + " " + version + ext
        new_file = os.path.join(tail, new_name)
        os.rename(file, new_file)


def move_files_dir_up(root, depth=2):
    """ Move files one level up. """
    files = list_files(root, depth, ext=("eso", "err", "htm", "idf"))

    for file in files:
        new_path = os.path.dirname(os.path.dirname(file))
        shutil.move(file, new_path)


def replace_underscore(root, replace_char=" ", depth=3):
    """ Add version number to eso file name. """
    files = list_files(root, depth, ext=("eso", "err", "htm", "idf"))

    for file in files:
        tail, head = os.path.split(file)
        new_name = head.replace("_", replace_char)
        new_file = os.path.join(tail, new_name)
        os.rename(file, new_file)


def remove_empty_dirs(root):
    """ Removes empty directories. """
    for subdir, dirs, files in os.walk(root):
        for dir in dirs:
            os.rmdir(os.path.join(subdir, dir))


def rename(root, old_str, new_str, ext, depth):
    files = list_files(root, depth, ext=ext)
    for file in files:
        tail, head = os.path.split(file)
        new_name = head.replace(old_str, new_str)
        new_file = os.path.join(tail, new_name)
        os.rename(file, new_file)


def write_results(df, writer, sheet_name="sheet"):
    try:
        df.to_excel(writer, sheet_name=sheet_name)

    except AttributeError:
        print("No results to print for sheet: {}".format(sheet_name))

import words
from collections import defaultdict

book = words.get_words('/Users/miraekim/workspace/coursework/map543/project/books/')
word_list = list(book)

def write_file(desired_file_size, dictionary):
    text_file = ''
    while len(text_file) < desired_file_size:
        sentence = ' '.join([random.choice(dictionary) for x in range(10)]) + ' '
        text_file += sentence
    return text_file


def create_dictionary_of_word_count(input_text):
    input_text_list = input_text.split(' ')
    count_dict = defaultdict(int)
    for word in input_text_list:
        count_dict[word] += 1
    return count_dict


def create_many_files(main_dir,each_file_size,num_of_docs):
    meta_file_input = "/Users/miraekim/workspace/coursework/map543/project/{main_dir}/meta_data_file.py".format(main_dir=main_dir)
    meta_file = open(meta_file_input, "w")

    for i in range(num_of_docs):
        test_str = write_file(each_file_size, filtered_word_list)
        input_file = "/Users/miraekim/workspace/coursework/map543/project/{main_dir}/doc_{txt_iter}.txt".format(main_dir=main_dir,txt_iter=i+1)
        text_file = open(input_file, "w")
        text_file.write(test_str)

        meta_stats_str = dict(create_dictionary_of_word_count(test_str))
        meta_file = open(meta_file_input, "a")
        meta_file.write('{'+"'doc_{txt_iter}'".format(  txt_iter=i+1) +': '+ str(meta_stats_str) + '}\n')
        text_file.close()

# Test 
# create_many_files('doc_set_20mb',20000000,5)
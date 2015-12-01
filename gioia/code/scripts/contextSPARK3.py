import re
import math
from scipy.stats import poisson
import itertools
import time

# Initialize Spark
from pyspark import SparkContext
sc = SparkContext()
sc.setLogLevel('ERROR')

######################
#
# DOCUMENTATION HERE
#
######################

# number of partitions to be used
n_partitions = 6
MAX_EDIT_DISTANCE = 3

def get_n_deletes_list(w, n):
    '''
    Given a word, derive list of strings with up to n characters deleted
    '''
    # since this list is generally of the same magnitude as the number of 
    # characters in a word, it may not make sense to parallelize this
    # so we use python to create the list
    deletes = []
    queue = [w]
    for d in range(n):
        temp_queue = []
        for word in queue:
            if len(word)>1:
                for c in range(len(word)):  # character index
                    word_minus_c = word[:c] + word[c+1:]
                    if word_minus_c not in deletes:
                        deletes.append(word_minus_c)
                    if word_minus_c not in temp_queue:
                        temp_queue.append(word_minus_c)
        queue = temp_queue
        
    return deletes

def get_transitions(sentence):
    if len(sentence)<2:
        return None
    else:
        return [((sentence[i], sentence[i+1]), 1) 
                for i in range(len(sentence)-1)]
    
def map_transition_prob(x):
    vals = x[1]
    total = float(sum(vals.values()))
    probs = {k: math.log(v/total) for k, v in vals.items()}
    return (x[0], probs)

def parallel_create_dictionary(fname):
    '''
    Create dictionary, start probabilities and transition
    probabilities using Spark RDDs.
    '''
    # we generate and count all words for the corpus,
    # then add deletes to the dictionary
    # this is a slightly different approach from the SymSpell algorithm
    # that may be more appropriate for Spark processing
    
    ############
    #
    # load file & initial processing
    #
    ############
    
    # http://stackoverflow.com/questions/22520932/python-remove-all-non-alphabet-chars-from-string
    regex = re.compile('[^a-z ]')

    # convert file into one long sequence of words
    make_all_lower = sc.textFile(fname) \
            .map(lambda line: line.lower()) \
            .filter(lambda x: x!='').cache()
    
    # split into individual sentences and remove other punctuation
    split_sentence = make_all_lower.flatMap(lambda line: line.split('.')) \
            .map(lambda sentence: regex.sub(' ', sentence)) \
            .map(lambda sentence: sentence.split()).cache()
    
    ############
    #
    # generate start probabilities
    #
    ############
    
    # only focus on words at the start of sentences
    start_words = split_sentence.map(lambda sentence: sentence[0] if len(sentence)>0 else None) \
            .filter(lambda word: word!=None)
    
    # add a count to each word
    count_start_words_once = start_words.map(lambda word: (word, 1)).cache()

    # use accumulator to count the number of words at the start of sentences
    accum_total_start_words = sc.accumulator(0)
    count_total_start_words = count_start_words_once.foreach(lambda x: accum_total_start_words.add(1))
    total_start_words = float(accum_total_start_words.value)
    
    # reduce into count of unique words at the start of sentences
    unique_start_words = count_start_words_once.reduceByKey(lambda a, b: a + b)
    
    # convert counts to probabilities
    start_prob_calc = unique_start_words.mapValues(lambda v: math.log(v/total_start_words))
    
    # get default start probabilities (for words not in corpus)
    default_start_prob = math.log(1/total_start_words)
    
    # store start probabilities as a dictionary (will be used as a lookup table)
    start_prob = start_prob_calc.collectAsMap()
    
    ############
    #
    # generate transition probabilities
    #
    ############
    
    # focus on continuous word pairs within the sentence
    # e.g. "this is a test" -> "this is", "is a", "a test"
    # note: as the relevant probability is P(word|previous word)
    # the tuples are ordered as (previous word, word)
    other_words = split_sentence.map(lambda sentence: get_transitions(sentence)) \
            .filter(lambda x: x!=None). \
            flatMap(lambda x: x).cache()

    # use accumulator to count the number of transitions
    accum_total_other_words = sc.accumulator(0)
    count_total_other_words = other_words.foreach(lambda x: accum_total_other_words.add(1))
    total_other_words = float(accum_total_other_words.value)
    
    # reduce into count of unique word pairs
    unique_other_words = other_words.reduceByKey(lambda a, b: a + b)
    
    # aggregate by previous word
    # i.e. (previous word, [(word1, word1-previous word count), (word2, word2-previous word count), ...])
    other_words_collapsed = unique_other_words.map(lambda x: (x[0][0], (x[0][1], x[1]))) \
            .groupByKey().mapValues(dict)

    # POTENTIAL OPTIMIZATION: FIND AN ALTERNATIVE TO GROUPBYKEY (CREATES ~9.3MB SHUFFLE)
    
    # convert counts to probabilities
    transition_prob_calc = other_words_collapsed.map(lambda x: map_transition_prob(x))
    
    # get default transition probabilities (for word pairs not in corpus)
    default_transition_prob = math.log(1/total_other_words)
    
    # store transition probabilities as dictionary (will be used as lookup table)
    transition_prob = transition_prob_calc.collectAsMap()
    
    ############
    #
    # process corpus for dictionary
    #
    ############
    
    replace_nonalphs = make_all_lower.map(lambda line: regex.sub(' ', line))
    all_words = replace_nonalphs.flatMap(lambda line: line.split())

    # create core corpus dictionary (i.e. only words appearing in file, no "deletes") and cache it
    # output RDD of unique_words_with_count: [(word1, count1), (word2, count2), (word3, count3)...]
    count_once = all_words.map(lambda word: (word, 1))
    unique_words_with_count = count_once.reduceByKey(lambda a, b: a + b).cache()
    
    ############
    #
    # generate deletes list
    #
    ############
    
    # generate list of n-deletes from words in a corpus of the form: [(word1, count1), (word2, count2), ...]
     
    assert MAX_EDIT_DISTANCE > 0  
    
    generate_deletes = unique_words_with_count.map(lambda (parent, count): 
                                                   (parent, get_n_deletes_list(parent, MAX_EDIT_DISTANCE)))
    expand_deletes = generate_deletes.flatMapValues(lambda x: x)
    swap = expand_deletes.map(lambda (orig, delete): (delete, ([orig], 0)))
   
    ############
    #
    # combine delete elements with main dictionary
    #
    ############
    
    corpus = unique_words_with_count.mapValues(lambda count: ([], count))
    combine = swap.union(corpus)  # combine deletes with main dictionary, eliminate duplicates
    
    # since the dictionary will only be a lookup table once created, we can
    # pass on as a Python dictionary rather than RDD by reducing locally and
    # avoiding an extra shuffle from reduceByKey
    dictionary = combine.reduceByKeyLocally(lambda a, b: (a[0]+b[0], a[1]+b[1]))

    words_processed = unique_words_with_count.map(lambda (k, v): v) \
            .reduce(lambda a, b: a + b)
        
    word_count = unique_words_with_count.count()   
    
    # output stats
    print 'Total words processed: %i' % words_processed
    print 'Total unique words in corpus: %i' % word_count 
    print 'Total items in dictionary (corpus words and deletions): %i' % len(dictionary)
    print '  Edit distance for deletions: %i' % MAX_EDIT_DISTANCE
    print 'Total unique words at the start of a sentence: %i' \
        % len(start_prob)
    print 'Total unique word transitions: %i' % len(transition_prob)
    
    return dictionary, start_prob, default_start_prob, transition_prob, default_transition_prob

######################
#
# DOCUMENTATION HERE
#
######################

def dameraulevenshtein(seq1, seq2):
    '''
    Calculate the Damerau-Levenshtein distance between sequences.
    Same code as word-level checking.
    '''
    
    # codesnippet:D0DE4716-B6E6-4161-9219-2903BF8F547F
    # Conceptually, this is based on a len(seq1) + 1 * len(seq2) + 1
    # matrix. However, only the current and two previous rows are
    # needed at once, so we only store those.
    
    oneago = None
    thisrow = range(1, len(seq2) + 1) + [0]
    
    for x in xrange(len(seq1)):
        
        # Python lists wrap around for negative indices, so put the
        # leftmost column at the *end* of the list. This matches with
        # the zero-indexed strings and saves extra calculation.
        twoago, oneago, thisrow = \
            oneago, thisrow, [0] * len(seq2) + [x + 1]
        
        for y in xrange(len(seq2)):
            delcost = oneago[y] + 1
            addcost = thisrow[y - 1] + 1
            subcost = oneago[y - 1] + (seq1[x] != seq2[y])
            thisrow[y] = min(delcost, addcost, subcost)
            # This block deals with transpositions
            if (x > 0 and y > 0 and seq1[x] == seq2[y - 1]
                and seq1[x-1] == seq2[y] and seq1[x] != seq2[y]):
                thisrow[y] = min(thisrow[y], twoago[y - 2] + 1)
                
    return thisrow[len(seq2) - 1]

def get_suggestions(string, dictionary, longest_word_length=20, 
                    min_count=100, max_sug=10):
    '''
    Return list of suggested corrections for potentially incorrectly
    spelled word.
    Code based on get_suggestions function from word-level checking,
    with the addition of the min_count parameter, which only
    considers words that have occur more than min_count times in the
    (dictionary) corpus.
    '''
    
    if (len(string) - longest_word_length) > MAX_EDIT_DISTANCE:
        # to ensure Viterbi can keep running -- use the word itself
        return [(string, 0)]
    
    suggest_dict = {}
    
    queue = [string]
    q_dictionary = {}  # items other than string that we've checked
    
    while len(queue)>0:
        q_item = queue[0]  # pop
        queue = queue[1:]
        
        # process queue item
        if (q_item in dictionary) and (q_item not in suggest_dict):
            if (dictionary[q_item][1]>0):
            # word is in dictionary, and is a word from the corpus,
            # and not already in suggestion list so add to suggestion
            # dictionary, indexed by the word with value (frequency
            # in corpus, edit distance)
            # note: q_items that are not the input string are shorter
            # than input string since only deletes are added (unless
            # manual dictionary corrections are added)
                assert len(string)>=len(q_item)
                suggest_dict[q_item] = \
                    (dictionary[q_item][1], len(string) - len(q_item))
            
            # the suggested corrections for q_item as stored in
            # dictionary (whether or not q_item itself is a valid
            # word or merely a delete) can be valid corrections
            for sc_item in dictionary[q_item][0]:
                if (sc_item not in suggest_dict):
                    
                    # compute edit distance
                    # suggested items should always be longer (unless
                    # manual corrections are added)
                    assert len(sc_item)>len(q_item)
                    # q_items that are not input should be shorter
                    # than original string 
                    # (unless manual corrections added)
                    assert len(q_item)<=len(string)
                    if len(q_item)==len(string):
                        assert q_item==string
                        item_dist = len(sc_item) - len(q_item)

                    # item in suggestions list should not be the same
                    # as the string itself
                    assert sc_item!=string           
                    # calculate edit distance using Damerau-
                    # Levenshtein distance
                    item_dist = dameraulevenshtein(sc_item, string)
                    
                    if item_dist<=MAX_EDIT_DISTANCE:
                        # should already be in dictionary if in
                        # suggestion list
                        assert sc_item in dictionary  
                        # trim list to contain state space
                        if (dictionary[q_item][1]>0): 
                            suggest_dict[sc_item] = \
                                (dictionary[sc_item][1], item_dist)
        
        # now generate deletes (e.g. a substring of string or of a
        # delete) from the queue item as additional items to check
        # -- add to end of queue
        assert len(string)>=len(q_item)
        if (len(string)-len(q_item))<MAX_EDIT_DISTANCE \
            and len(q_item)>1:
            for c in range(len(q_item)): # character index        
                word_minus_c = q_item[:c] + q_item[c+1:]
                if word_minus_c not in q_dictionary:
                    queue.append(word_minus_c)
                    # arbitrary value to identify we checked this
                    q_dictionary[word_minus_c] = None

    # return list of suggestions: (correction, edit distance)
    
    # only include words that have appeared a minimum number of times
    # make sure that we do not lose the original word
    as_list = [i for i in suggest_dict.items() 
               if (i[1][0]>min_count or i[0]==string)]
    
    # only include the most likely suggestions (based on frequency
    # and edit distance from original word)
    trunc_as_list = sorted(as_list, 
            key = lambda (term, (freq, dist)): (dist, -freq))[:max_sug]
    
    if len(trunc_as_list)==0:
        # to ensure Viterbi can keep running
        # -- use the word itself if no corrections are found
        return [(string, 0)]
        
    else:
        # drop the word frequency - not needed beyond this point
        return [(i[0], i[1][1]) for i in trunc_as_list]

    '''
    Output format:
    get_suggestions('file', dictionary)
    [('file', 0), ('five', 1), ('fire', 1), ('fine', 1), ('will', 2),
    ('time', 2), ('face', 2), ('like', 2), ('life', 2), ('while', 2)]
    '''
    
def get_emission_prob(edit_dist, poisson_lambda=0.01):
    '''
    The emission probability, i.e. P(observed word|intended word)
    is approximated by a Poisson(k, l) distribution, where 
    k=edit distance and l=0.01.
    
    The lambda parameter matches the one used in the AM207
    lecture notes. Various parameters between 0 and 1 were tested
    to confirm that 0.01 yields the most accurate results.
    '''
    
    return math.log(poisson.pmf(edit_dist, poisson_lambda))

######################
# Multiple helper functions are used to avoid KeyErrors when
# attempting to access values that are not present in dictionaries,
# in which case the previously specified default value is returned.
######################

def get_start_prob(word, start_prob, default_start_prob):
    try:
        return start_prob[word]
    except KeyError:
        return default_start_prob
    
def get_transition_prob(cur_word, prev_word, transition_prob, 
                        default_transition_prob):
    try:
        return transition_prob[prev_word][cur_word]
    except KeyError:
        return default_transition_prob

def get_belief(prev_word, prev_belief):
    try:
        return prev_belief[prev_word]
    except KeyError:
        return math.log(math.exp(min(prev_belief.values()))/2.)  

def get_count_mismatches(sentences):
    orig_sentence, sug_sentence = sentences
    count_mismatches = len([(orig_sentence[i], sug_sentence[i]) 
            for i in range(len(orig_sentence))
            if orig_sentence[i]!=sug_sentence[i]])
    return count_mismatches, orig_sentence, sug_sentence

def get_sentence_word_id(words):
    return [(i, w) for i, w in enumerate(words)]

def split_sentence_words(sentence):
    sent_id, words = sentence
    return [[sent_id, w] for w in words]

def start_word_prob(words, tmp_sp, d_sp):
    orig_word, sug_words = words
    probs = [(w[0], math.exp(
                get_start_prob(w[0], tmp_sp, d_sp) 
                + get_emission_prob(w[1])
            )) 
             for w in sug_words]
    sum_probs = sum([p[1] for p in probs])
    probs = [([p[0]], math.log(p[1]/sum_probs)) for p in probs]
    return probs

def split_suggestions(sentence):
    sent_id, (word, word_sug)  = sentence
    return [[sent_id, (word, w)] for w in word_sug]

def subs_word_prob(words, tmp_tp, d_tp):
    
    # unpack values
    sent_id = words[0]
    cur_word = words[1][0][0]
    cur_sug = words[1][0][1][0]
    cur_sug_ed = words[1][0][1][1]
    prev_sug = words[1][1]
    
    # belief + transition probability + emission probability
    (prob, word) = max((p[1]
                 + get_transition_prob(cur_sug, p[0][-1], tmp_tp, d_tp)
                 + get_emission_prob(cur_sug_ed), p[0])
                     for p in prev_sug)
    
    return sent_id, (word + [cur_sug], math.exp(prob))

def normalize(probs):
    sum_probs = sum([p[1] for p in probs])
    return [(p[0], math.log(p[1]/sum_probs)) for p in probs]

def get_max_path(final_paths):
    max_path = max((p[1], p[0]) for p in final_paths)
    return max_path[1]

def correct_document_context_parallel_steps(fname, dictionary,
                             start_prob, default_start_prob,
                             transition_prob, default_transition_prob):
    
    ############
    #
    # load file & initial processing
    #
    ############
    
    # broadcast Python dictionaries to workers
    bc_dictionary = sc.broadcast(dictionary)
    bc_start_prob = sc.broadcast(start_prob)
    bc_transition_prob = sc.broadcast(transition_prob)
    
    # convert all text to lowercase and drop empty lines
    make_all_lower = sc.textFile(fname) \
        .map(lambda line: line.lower()) \
        .filter(lambda x: x!='')
    
    regex = re.compile('[^a-z ]')
    
    # split into sentences -> remove special characters -> convert into list of words
    split_sentence = make_all_lower.flatMap(lambda line: line.split('.')) \
            .map(lambda sentence: regex.sub(' ', sentence)) \
            .map(lambda sentence: sentence.split()).cache()
    
    # use accumulator to count the number of words checked
    accum_total_words = sc.accumulator(0)
    split_words = split_sentence.flatMap(lambda x: x) \
            .foreach(lambda x: accum_total_words.add(1))
    
    # assign each sentence a unique id
    sentence_id = split_sentence.zipWithIndex().map(lambda (k, v): (v, k)).cache()
    
    # count the number of words in each sentence
    sentence_word_count = sentence_id.mapValues(lambda v: len(v))
    
    ############
    #
    # spell-checking
    #
    ############

    # number each word in a sentence, and split into individual words
    sentence_word_id = sentence_id.mapValues(lambda v: get_sentence_word_id(v)) \
            .flatMap(lambda x: split_sentence_words(x))
    
    # get suggestions for each word
    sentence_word_suggestions = sentence_word_id.mapValues(lambda v: 
                                            (v[0], v[1], get_suggestions(v[1], bc_dictionary.value))).cache()
    
    # filter for the first words in sentences
    sentence_word_1 = sentence_word_suggestions.filter(lambda (k, v): v[0]==0) \
            .mapValues(lambda v: (v[1], v[2]))
    
    # calculate probability for each suggestion
    # format: (sentence id, [path-probability pairs])
    sentence_path = sentence_word_1.mapValues(lambda v: 
                                              start_word_prob(v, bc_start_prob.value, default_start_prob))
    # start from second word (zero-indexed)
    word_num = 1
    
    # extraact any sentences that are ready
    completed = sentence_word_count.filter(lambda (k, v): v==word_num) \
            .join(sentence_path).mapValues(lambda v: v[1])
    
    # filter for the next words in sentences
    sentence_word_next = sentence_word_suggestions.filter(lambda (k,v): v[0]==word_num) \
            .mapValues(lambda v: (v[1], v[2]))
    
    # check that there are more words left
    while not sentence_word_next.isEmpty():

        # split into suggestions
        sentence_word_next_split = sentence_word_next.flatMap(lambda x: split_suggestions(x))
        
        # join on previous path
        # format: (sentence id, ((current word, (current word suggestion, edit distance)), 
        #         [(previous path-probability pairs)]))
        sentence_word_next_path = sentence_word_next_split.join(sentence_path)
        
        # calculate path with max probability
        sentence_word_next_path_prob = sentence_word_next_path.map(lambda x:
                                                subs_word_prob(x, bc_transition_prob.value, default_transition_prob))
        
        # normalize for numerical stability
        sentence_path = sentence_word_next_path_prob.groupByKey().mapValues(lambda v: normalize(v))
        
        # move on to next word
        word_num += 1
        
        # extract any sentences that are ready
        completed = completed.union(
            sentence_word_count.filter(lambda (k, v): v==word_num) \
            .join(sentence_path) \
            .mapValues(lambda v: v[1]))
        
        # filter for the next words in sentences
        sentence_word_next = sentence_word_suggestions.filter(lambda (k, v): v[0]==word_num) \
                .mapValues(lambda v: (v[1], v[2]))
        
    # get most likely path (sentence)
    sentence_suggestion = completed.mapValues(lambda v: get_max_path(v))

    # join with original path (sentence)
    sentence_max_prob = sentence_id.join(sentence_suggestion)
        
    ############
    #
    # output results
    #
    ############
    
    # count the number of errors per sentence, drop any sentences without errors
    sentence_errors = sentence_max_prob.mapValues(lambda v: (get_count_mismatches(v))) \
            .filter(lambda (k, v): v[0]>0).cache()
               
    # collect all sentences with identified errors
    sentence_errors_list = sentence_errors.collect()
    
    # number of potentially misspelled words
    num_errors = sum([s[1][0] for s in sentence_errors_list])
    
    # print identified errors (eventually output to file)
    for sentence in sentence_errors_list:
        print 'Sentence %i: %s --> %s' % (sentence[0], ' '.join(sentence[1][1]), ' '.join(sentence[1][2]))
    
    print '-----'
    print 'Total words checked: %i' % accum_total_words.value
    print 'Total potential errors found: %i' % num_errors

if __name__ == '__main__':

    ############
    #
    # pre-processing
    #
    ############

	dictionary_file = 'testdata/big.txt'

	print 'Creating dictionary with %s...' % dictionary_file

	start_time = time.time()

	dictionary, start_prob, default_start_prob, transition_prob, default_transition_prob = \
	    parallel_create_dictionary(dictionary_file)

	run_time = time.time() - start_time

	print '%.2f seconds to run' % run_time

    ############
    #
    # spell-checking
    #
    ############

	check_file = 'testdata/test.txt'

	start_time = time.time()

	print 'Spell-checking %s...' % check_file

	correct_document_context_parallel_steps(check_file, dictionary,
	        start_prob, default_start_prob, transition_prob, default_transition_prob)

	run_time = time.time() - start_time

	print '%.2f seconds to run' % run_time


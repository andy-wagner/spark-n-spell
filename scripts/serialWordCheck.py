import re
import math
from scipy.stats import poisson

def dameraulevenshtein(seq1, seq2):
    """
    Calculate the Damerau-Levenshtein distance between sequences.

    Source: http://mwh.geek.nz/2009/04/26/python-damerau-levenshtein-distance/
    
    This distance is the number of additions, deletions, substitutions,
    and transpositions needed to transform the first sequence into the
    second. Although generally used with strings, any sequences of
    comparable objects will work.

    Transpositions are exchanges of *consecutive* characters; all other
    operations are self-explanatory.

    This implementation is O(N*M) time and O(M) space, for N and M the
    lengths of the two sequences.

    >>> dameraulevenshtein('ba', 'abc')
    2
    >>> dameraulevenshtein('fee', 'deed')
    2

    It works with arbitrary sequences too:
    >>> dameraulevenshtein('abcd', ['b', 'a', 'c', 'd', 'e'])
    2
    """
    # codesnippet:D0DE4716-B6E6-4161-9219-2903BF8F547F
    # Conceptually, this is based on a len(seq1) + 1 * len(seq2) + 1 matrix.
    # However, only the current and two previous rows are needed at once,
    # so we only store those.
    oneago = None
    thisrow = range(1, len(seq2) + 1) + [0]
    for x in xrange(len(seq1)):
        # Python lists wrap around for negative indices, so put the
        # leftmost column at the *end* of the list. This matches with
        # the zero-indexed strings and saves extra calculation.
        twoago, oneago, thisrow = oneago, thisrow, [0] * len(seq2) + [x + 1]
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

def get_suggestions(string, dictionary, longest_word_length, max_edit_distance, silent=False, min_count=0):
    '''return list of suggested corrections for potentially incorrectly spelled word'''
    
    if (len(string) - longest_word_length) > max_edit_distance:
        if not silent:
            print 'No items in dictionary within maximum edit distance'
        return []
    
    # suggestions = []
    # s_dictionary = {}
    suggest_dict = {}
    
    queue = [string]
    q_dictionary = {}  # items other than string that we've checked
    
    while len(queue)>0:
        q_item = queue[0]  # pop
        queue = queue[1:]
        
        # process queue item
        if (q_item in dictionary) and (q_item not in suggest_dict):
            if (dictionary[q_item][1]>=min_count):# GD added to trim list for context checking
            # word is in dictionary, and is a word from the corpus, and not already in suggestion list
            # so add to suggestion dictionary, indexed by the word with value (frequency in corpus, edit distance)
            # note q_items that are not the input string are shorter than input string 
            # since only deletes are added (unless manual dictionary corrections are added)
                assert len(string)>=len(q_item)
                suggest_dict[q_item] = (dictionary[q_item][1], len(string) - len(q_item))

            ## the suggested corrections for q_item as stored in dictionary (whether or not
            ## q_item itself is a valid word or merely a delete) can be valid corrections
            for sc_item in dictionary[q_item][0]:
                if (sc_item not in suggest_dict):
                    # compute edit distance
                    # if len(sc_item)==len(q_item):
                    #    item_dist = len(string) - len(q_item)
                    # suggested items should always be longer (unless manual corrections are added)
                    assert len(sc_item)>len(q_item)
                    # q_items that are not input should be shorter than original string 
                    # (unless manual corrections added)
                    assert len(q_item)<=len(string)
                    if len(q_item)==len(string):
                        assert q_item==string
                        item_dist = len(sc_item) - len(q_item)
                    #elif len(q_item)==len(string):
                        # a suggestion could be longer or shorter than original string (bug in original FAROO?)
                        # if suggestion is from string's suggestion list, sc_item will be longer
                        # if suggestion is from a delete's suggestion list, sc_item may be shorter
                    #   item_dist = abs(len(sc_item) - len(q_item))
                    #else:
                    # check in original code, but probably not necessary because string has already checked
                    assert sc_item!=string

                    # calculate edit distance using, for example, Damerau-Levenshtein distance
                    item_dist = dameraulevenshtein(sc_item, string)

                    if item_dist <= max_edit_distance:
                        assert sc_item in dictionary  # should already be in dictionary if in suggestion list
                        if (dictionary[q_item][1]>=min_count):# GD added to trim list for context checking
                            suggest_dict[sc_item] = (dictionary[sc_item][1], item_dist)

        # now generate deletes (e.g. a substring of string or of a delete) from the queue item
        # as additional items to check -- add to end of queue
        assert len(string)>=len(q_item)
        if (len(string)-len(q_item)) < max_edit_distance and len(q_item)>1:
            for c in range(len(q_item)): # character index        
                word_minus_c = q_item[:c] + q_item[c+1:]
                if word_minus_c not in q_dictionary:
                    queue.append(word_minus_c)
                    q_dictionary[word_minus_c] = None  # arbitrary value, just to identify we checked this
             
    # queue is now empty: convert suggestions in dictionary to list for output
    
    if not silent:
        print 'Number of possible corrections: %i' % len(suggest_dict)
        print '  Edit distance for deletions: %i' % max_edit_distance
    
    # output option 1
    # sort results by ascending order of edit distance and descending order of frequency
    #     and return list of suggested corrections only:
    # return sorted(suggest_dict, key = lambda x: (suggest_dict[x][1], -suggest_dict[x][0]))

    # output option 2
    # return list of suggestions with (correction, (frequency in corpus, edit distance)):
    as_list = suggest_dict.items()
    return sorted(as_list, key = lambda (term, (freq, dist)): (dist, -freq))

    '''
    Option 1:
    get_suggestions('file')
    ['file', 'five', 'fire', 'fine', ...]
    
    Option 2:
    get_suggestions('file')
    [('file', (5, 0)),
     ('five', (67, 1)),
     ('fire', (54, 1)),
     ('fine', (17, 1))...]  
    '''

# get best word
def best_word(s, dictionary, longest_word_length, max_edit_distance, silent=False):
    try:
        return get_suggestions(s, dictionary, longest_word_length, max_edit_distance, silent=silent)[0]
    except:
        return None

def correct_document(fname, dictionary, longest_word_length, max_edit_distance):

    with open(fname) as file:

        doc_word_count = 0
        corrected_word_count = 0
        unknown_word_count = 0
        
        print "Finding misspelled words in your document..." 
        
        for i, line in enumerate(file):

            doc_words = re.findall('[a-z]+', line.lower())  # separate by words by non-alphabetical characters      
            
            for doc_word in doc_words:
                
                doc_word_count += 1
                suggestion = best_word(doc_word, dictionary, longest_word_length, max_edit_distance, silent=True)
                
                if suggestion is None:
                    print 'In line %i, the word < %s > was not found (no suggested correction)' % (i, doc_word)
                    unknown_word_count += 1
                
                elif suggestion[0]!=doc_word:
                    print 'In line %i, %s: suggested correction is < %s >' % (i, doc_word, suggestion[0])
                    corrected_word_count += 1
        
    print '-----'
    print 'Total words checked: %i' % doc_word_count
    print 'Total unknown words: %i' % unknown_word_count
    print 'Total potential errors found: %i' % corrected_word_count
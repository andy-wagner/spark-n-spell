import re

def get_deletes_list(w, max_edit_distance):
    '''given a word, derive strings with up to max_edit_distance characters deleted'''
    
    deletes = []
    queue = [w]
    
    for d in range(max_edit_distance):
        
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

def create_dictionary_entry(w, dictionary, longest_word_length, max_edit_distance):
    '''add word and its derived deletions to dictionary'''

    # check if word is already in dictionary
    # dictionary entries are in the form: (list of suggested corrections, frequency of word in corpus)

    new_real_word_added = False
    
    if w in dictionary:
        dictionary[w] = (dictionary[w][0], dictionary[w][1] + 1)  # increment count of word in corpus
    else:
        dictionary[w] = ([], 1)  
        longest_word_length = max(longest_word_length, len(w))
        
    if dictionary[w][1]==1:
        # first appearance of word in corpus
        # n.b. word may already be in dictionary as a derived word (deleting character from a real word)
        # but counter of frequency of word in corpus is not incremented in those cases)
        
        new_real_word_added = True
        deletes = get_deletes_list(w, max_edit_distance)
        
        for item in deletes:
            
            if item in dictionary:
                # add (correct) word to delete's suggested correction list if not already there
                if item not in dictionary[item][0]:
                    dictionary[item][0].append(w)
            else:
                dictionary[item] = ([w], 0)  # note frequency of word in corpus is not incremented
        
    return new_real_word_added, longest_word_length

def create_dictionary(fname, max_edit_distance):

	dictionary = dict()
	longest_word_length = 0
	word_count = 0

	print "\nCreating dictionary..." 

	with open(fname) as file:

		for line in file:

			words = re.findall('[a-z]+', line.lower())  # separate by words by non-alphabetical characters      

			for word in words:
				
				new_word, longest_word_length = \
					create_dictionary_entry(word, dictionary, longest_word_length, max_edit_distance)
        		
				if new_word:
					word_count += 1

	print 'Total unique words in corpus: %i' % word_count
	print 'Total items in dictionary (corpus words and deletions): %i' % len(dictionary)
	print '  Edit distance for deletions: %i' % max_edit_distance
	print '  Length of longest word in corpus: %i' % longest_word_length
        
	return dictionary, longest_word_length
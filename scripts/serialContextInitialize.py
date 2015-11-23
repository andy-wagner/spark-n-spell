import re
import math
from scipy.stats import poisson
import serialWordInitialize as swi

#########################################################################################################
# CONTEXT-LEVEL CORRECTION
#
# Each sentence is modeled as a hidden Markov model. Prior probabilities (for first word in the sentence)
# and transition probabilities (for all subsequent words) are calculated when generating the main
# dictionary, using the same corpus. Emission probabilities are generated on the fly by parameterizing
# a Poisson distribution with the edit distance. The state space of possible corrections is based on the
# suggested words from the word-level correction.
#
# All probabilities are stored in log-space to avoid underflow. Pre-defined minimum values are used for
# words that are not present in the dictionary and/or probability tables.
#########################################################################################################


def create_dictionary(fname, max_edit_distance):
    
    print '\nCreating dictionary...' 

    dictionary = dict()
    longest_word_length = 0
    start_prob = dict()
    transition_prob = dict()
    word_count = 0
    
    with open(fname) as file:    
        
        for line in file:
            
            for sentence in line.split('.'):
                
                # Separate by words by non-alphabetical characters
                words = re.findall('[a-z]+', sentence.lower())       
                
                for w, word in enumerate(words):
                    
                    new_word, longest_word_length = \
                        swi.create_dictionary_entry(word, dictionary, longest_word_length, max_edit_distance)
                    
                    if new_word:
                        word_count += 1
                        
                    # Calculates probabilities for Hidden Markov Model
                    if w == 0:

                        # Probability of a word being at the beginning of a sentence
                        if word in start_prob:
                            start_prob[word] += 1
                        else:
                            start_prob[word] = 1
                    else:
                        
                        # Probability of transitionining from one word to another
                        # dictionary format: {previous word: {word1 : P(word1|prevous word), word2 : P(word2|prevous word)}}
                        
                        # Check that prior word is present - create if not
                        if words[w - 1] not in transition_prob:
                            transition_prob[words[w - 1]] = dict()
                            
                        # Check that current word is present - create if not
                        if word not in transition_prob[words[w - 1]]:
                            transition_prob[words[w - 1]][word] = 0
                            
                        # Update value
                        transition_prob[words[w - 1]][word] += 1
                              
    # Converts counts to log-probabilities (to avoid underflow)
    # Note: natural logarithm, not base-10
    total_start_words = float(sum(start_prob.values()))
    default_start_prob = math.log(1/total_start_words)
    start_prob.update({k: math.log(v/total_start_words) for k, v in start_prob.items()})
    default_transition_prob = math.log(1./word_count)    
    transition_prob.update({k: {k1: math.log(float(v1)/sum(v.values())) for k1, v1 in v.items()} \
                            for k, v in transition_prob.items()})

    print 'Total unique words in corpus: %i' % word_count
    print 'Total items in dictionary (corpus words and deletions): %i' % len(dictionary)
    print '  Edit distance for deletions: %i' % max_edit_distance
    print '  Length of longest word in corpus: %i' % longest_word_length
    print 'Total unique words appearing at the start of a sentence: %i' % len(start_prob)
    print 'Total unique word transitions: %i' % len(transition_prob)
        
    return dictionary, longest_word_length, start_prob, default_start_prob, transition_prob, default_transition_prob
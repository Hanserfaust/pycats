# -*- coding: utf-8 -*-
__author__ = 'hans'

from models import BlobIndexDTO
import string
import re
import pytz

# Can index a blob that is a valid utf-8 string. If blob is not a valid utf-8 (ie. a png-imgage, skip
# this and create a few tags manually)
class StringIndexer():
    white_list = string.letters + string.digits + ' '

    def __init__(self, index_depth=5):
        self.index_depth = index_depth

    # Takes a string and returns a clean string of lower-case words only
    # Makes a good base to create index from
    def strip_and_lower(self, string):
        # Split only on a couple of separators an do lower
        r1 = re.sub('[,\.\-\?=!@#$\(\)<>_\[\]\'\"\´\:]', ' ', string.lower())
        r2 = ' '.join(r1.split())
        return r2.encode('utf-8')

    # Will split a sting and return the permutations given depth
    # made for storing short scentences too use as index
    #
    # Example, given 'hello indexed words' and depth = 2
    # Will return: ['hello', 'indexed', 'words', 'hello indexed', 'indexed words']
    # ie depth equals the maximum number of words in a substring
    #
    # Assume string can be split on space, depth is an integer >= 1
    def _build_substrings(self, string, depth):
        result = set()
        words = string.split()

        for d in range(0, depth):
            for i in range(0, len(words)):
                if i+d+1 > len(words):
                    continue
                current_words = words[i:i+d+1]
                result.add(' '.join(current_words))
        return result

    # idea: could add flag to run the loop again but with ommited source_id and/or dataname to
    # return double and tripple amount of keys to make a global search available
    def build_indexes_from_timstamped_dto(self, dto, blob_data_row_key):
        if dto.str_for_index:
            indexable_string = self.strip_and_lower(dto.str_for_index)
        else:
            indexable_string = self.strip_and_lower(dto.data_value)

        substrings = self._build_substrings(indexable_string, self.index_depth)

        index_dtos = []
        for substring in substrings:
            index_dto = BlobIndexDTO(dto.source_id, dto.data_name, substring, dto.timestamp, blob_data_row_key)
            index_dtos.append(index_dto)

        return index_dtos

    def __datetime_to_utc(self, a_datetime):
        if a_datetime.tzinfo:
            # Convert to UTC if timezone info
            return a_datetime.astimezone (pytz.utc)
        else:
            # Assume datetime was in UTC if no timezone info exists
            return a_datetime

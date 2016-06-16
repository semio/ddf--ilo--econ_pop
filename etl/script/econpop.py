# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import re
import os
from ddf_utils.str import to_concept_id
from ddf_utils.index import create_index_file


# configuration of file path
source = '../source/EAPEP - complete dataset.txt'  # data
desc = '../source/Variable decription.txt'  # column description

out_dir = '../../'


def extract_concepts(txt):
    """create concept dataframe from source.

    parameters:

    txt: descriptions
    """
    cdf = pd.DataFrame([], columns=['concept', 'concept_type', 'name'])

    conc = []
    cname = []

    for i in txt.split('\n')[3:]:
        sp = re.split(' {2,}', i)
        conc.append(sp[0])
        cname.append(sp[1])

    # only keep needed data from description file.
    idx = conc.index('MFPR')
    cdf['concept'] = conc[:idx+1]
    cdf['name'] = cname[:idx+1]

    # manually set some of porperties
    cdf['concept_type'] = 'string'
    cdf['concept_type'].iloc[6:] = 'measure'
    cdf['concept_type'].iloc[4] = 'time'
    cdf['concept_type'].iloc[5] = 'entity_domain'

    cdf['concept'].iloc[1] = 'name'
    cdf['concept'].iloc[3] = 'country'
    cdf['name'].iloc[1] = 'Name'

    cdf['concept'] = cdf['concept'].map(to_concept_id)
    return cdf


def extract_entities_country(data):
    country = data[['iso3', 'iso2', 'country', 'sort_code']].copy()
    country = country.drop_duplicates()
    country.columns = ['country', 'iso2', 'name', 'sort_code']
    country['country'] = country['country'].map(to_concept_id)
    return country


def extract_entities_age_group(data):
    age_group = data[['age_group']].copy()
    age_group['name'] = 'Aged ' + age_group['age_group']
    age_group = age_group.drop_duplicates()
    age_group['age_group'] = age_group['age_group'].map(to_concept_id)
    return age_group


def extract_datapoints(data, cdf):
    # cdf: concept dataframe
    # cdf.ix[6:] are continuous concepts.
    dps_col = [*['iso3', 'year', 'age_group'], *cdf['concept'].values[6:]]

    data.columns = list(map(to_concept_id, data.columns))
    dps = data[dps_col].copy()

    dps['age_group'] = dps['age_group'].map(to_concept_id)
    dps['iso3'] = dps['iso3'].map(to_concept_id)
    dps = dps.rename(columns={'iso3': 'country'})
    dps = dps.set_index(['country', 'year', 'age_group'])

    for i, df in dps.items():
        df = df.dropna().reset_index()
        yield i, df


if __name__ == '__main__':
    print('reading source files...')
    data = pd.read_csv(source, delimiter='\t', encoding='iso-8859-1')
    data = data.loc[:, :'MFPR']  # drop unneeded columns

    txt = open(desc).read()

    print('creating concept files...')
    concepts = extract_concepts(txt)
    path = os.path.join(out_dir, 'ddf--concepts.csv')
    concepts.to_csv(path, index=False)

    print('creating entities files...')
    country = extract_entities_country(data)
    path = os.path.join(out_dir, 'ddf--entities--country.csv')
    country.to_csv(path, index=False)

    age = extract_entities_age_group(data)
    path = os.path.join(out_dir, 'ddf--entities--age_group.csv')
    age.to_csv(path, index=False)

    print('creating datapoints files...')
    for i, df in extract_datapoints(data, concepts):
        path = os.path.join(out_dir, 'ddf--datapoints--{}--by--country--age_group--year.csv'.format(i))
        df.to_csv(path, index=False, float_format='%g')

    print('creating index file...')
    create_index_file(out_dir)

    print('Done.')




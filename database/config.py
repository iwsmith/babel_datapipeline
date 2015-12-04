#!/usr/bin/env python
from __future__ import print_function
from configobj import ConfigObj, flatten_errors
from validate import Validator
import os

CONF_SPEC = "confspec.cfg"
CONF_DIR = "config"

STAGES = ("development", "beta", "unittest")
STAGE_ENV = "EIGENREC_STAGE"
PRODUCTS = ("plos", "jstor", "arxiv", "pubmed", "dblp", "ssrn", "mas", "aminer")
PRODUCT_ENV = "EIGENREC_PRODUCT"

# called by config_factory
def generate_config(stage, default="default"):
    default_config = ConfigObj(infile=os.path.join(CONF_DIR,
                                                   default + ".cfg"),
                               unrepr=True,
                               interpolation="template",
                               configspec=os.path.join(CONF_DIR,
                                                       CONF_SPEC))
    user_config = ConfigObj(infile=os.path.join(CONF_DIR,
                                                str(stage) + ".cfg"),
                            unrepr=True)
    #TODO: This should throw an error when the ENV isn't set
    default_config['metadata']['password'] = os.getenv('EIGENREC_MD_PW')
    default_config['metadata']['username'] = os.getenv('EIGENREC_MD_USERNAME')
    default_config.merge(user_config)

    return default_config

# called by luigified transformer main function
def config_factory(stage, verbose=False):
    config = generate_config(stage)
    vtor = Validator()
    if verbose is True:
        res = config.validate(vtor, preserve_errors=True)
        for entry in flatten_errors(config, res):
            # each entry is a tuple
            section_list, key, error = entry
            if key is not None:
                section_list.append(key)
            else:
                section_list.append('[missing section]')
            section_string = ', '.join(section_list)
            if error == False:
                error = 'Missing value or section.'
            print(section_string, ' = ', error)
    else:
        if config.validate(vtor) is not True: # Return is True or a dict.
            raise RuntimeError("Error in config file. Run config.py -v to validate")

    return config.dict()

def config_from_env():
    return os.getenv('EIGENREC_STAGE').lower()

# this function isn't needed for luigi process
def get_config(stage, headless=False, verbose=False):
    if args and args.stage:
        stage = args.stage
    else:
        try:
            stage = config_from_env()
        except AttributeError:
            import sys
            print("ERROR: %s missing from env. Either set it in env or as a command line argument." % (STAGE_ENV))
            print("Stage should be in %s as one of %s" % (STAGE_ENV, STAGES))
            sys.exit(1)

    if stage == 'beta' and not headless:
        import time
        print("WARNING " * 15)
        print("YOU ARE ABOUT TO MODIFY THE FOLLOWING STAGE: %s" % (stage))
        print("THIS IS A LIVE STAGE. PEOPLE WILL SEE THIS")
        print("You have 5 seconds to press ctrl+c to abort this action")
        print("WARNING " * 15)
        time.sleep(5)

    return config_factory(stage, verbose=verbose)


# for debugging
def add_prod_stage_to_parser(parser):
    parser.add_argument("-s", "--stage", help="Stage to display, overrides ENV settings.", choices=STAGES)
    return parser

# for debuging
if __name__ == '__main__':
    import argparse
    import pprint

    parser = argparse.ArgumentParser(description="Configuration for different stages")
    add_prod_stage_to_parser(parser)
    parser.add_argument("-v", "--validate", action="store_true", help="Validiate config for stage")
    args = parser.parse_args()

    conf = get_config(args, headless=True, verbose=True)
    pprint.PrettyPrinter().pprint(conf)

    if args.validate:
        config_factory(args.stage, verbose=True)

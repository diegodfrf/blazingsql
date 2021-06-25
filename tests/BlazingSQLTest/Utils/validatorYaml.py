import yamale
import os


def validate_config(filename):
    cwd = os.path.dirname(os.path.realpath(__file__))
    file_schema = 'schema_config.yaml'
    schema = yamale.make_schema(cwd + '/../Configuration/' + file_schema)

    # try the full path: don't force the user to use Configuration folder
    if not os.path.isfile(filename):
      filename = cwd + '/../Configuration/' + filename

    try:
        data = yamale.make_data(filename)
        yamale.validate(schema, data)
        print('Validation success for file "' + filename + '" with schema "' + file_schema + '"')
    except ValueError as e:
        print('Validation failed! for file "' + filename + '" with schema "' + file_schema + '"')
        print('\n%s' % str(e))
        exit(1)
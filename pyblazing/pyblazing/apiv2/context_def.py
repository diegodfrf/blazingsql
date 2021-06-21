def has_cudf():
  try: # TODO percy cordova improve this approach
    import cudf
  except ImportError as error:
    return False
  return True

# NOTE WARNING NEVER CHANGE THIS FIRST LINE!!!! NEVER EVER

def has_dask_cudf():
  try: # TODO percy cordova improve this approach
    import dask_cudf
  except ImportError as error:
    return False
  return True

'''
if has_cudf():
  import cudf
else:
  pass


if has_dask_cudf():
  import dask_cudf
else:
  pass
'''
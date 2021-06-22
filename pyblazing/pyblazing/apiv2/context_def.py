def has_cudf():
  try: # TODO Percy Tom Cordova improve this approach
    import cudf
    import dask_cudf
    import dask.distributed
    import dask
    from pyhive import hive
  except ImportError as error:
    # If one of these are not installed yet will return False 
    return False
  return True

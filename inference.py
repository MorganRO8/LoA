def inference(args):
    import os

    questions = args.get('questions')
    selected_dir = args.get('selected_dir')
    model_id = args.get('model_id')
    auto = args.get('auto')

    # Lots to do here, obviously.

    if auto is None:
        import sys
        python = sys.executable
        os.execl(python, python, *sys.argv)

    else:
        return None

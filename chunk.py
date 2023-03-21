def chunkedinf(tokenizer, full_start_token, z, question, model_path):

    from transformers import MegatronBertModel
    import torch

    print("now working on chunk " + str(z))
    # load model
    model = MegatronBertModel.from_pretrained(model_path)

    # create chunk
    chunk = full_start_token[((z-1)*400):(z*400)-1]

    # tokenize question and chunk
    input_ids = []
    question_tokens = tokenizer.encode(question)
    input_ids = question_tokens + chunk

    # create tokens list for each for later
    tokens = tokenizer.convert_ids_to_tokens(input_ids)

    # calculate number of tokens in segment A
    num_seg_a = input_ids.index(tokenizer.sep_token_id) + 1

    # calculate number of tokens in segment B
    num_seg_b = len(input_ids) - num_seg_a

    # create segment IDs
    segment_ids = [0] * num_seg_a + [1] * num_seg_b

    # check that segment IDs are correct length
    assert len(segment_ids) == len(input_ids)

    # run model
    start_scores, end_scores = model(torch.tensor([input_ids]),  # The tokens representing our input text.
                                     token_type_ids=torch.tensor([segment_ids]),
                                     return_dict=False)  # The segment IDs to differentiate question from answer_text


    # find start and end indices of answer
    answer_start = torch.argmax(start_scores)
    answer_end = torch.argmax(end_scores)

    # find start and end values of answer
    answer_start_value = torch.max(start_scores)

    answer_end_value = torch.max(end_scores)

    # calculate average value of answer
    average_value = (answer_start_value + answer_end_value) / 2

    if answer_start < len(tokens):
        answer = tokens[answer_start]
        for i in range(answer_start + 1, answer_end + 1):

            # If it's a subword token, then recombine it with the previous token.
            if tokens[i][0:2] == '##':
                answer += tokens[i][2:]

            # Otherwise, add a space then the token.
            else:
                answer += ' ' + tokens[i]

    else:
        answer = ""

    print("chunk " + str(z) + " complete")

    return answer, average_value

def chunkedinfoffs(tokenizer, full_start_token, z, question, model_path):
    from transformers import MegatronBertModel
    import torch

    # load model
    model = MegatronBertModel.from_pretrained(model_path)

    # create chunk
    chunk = full_start_token[((z-1)*400)+200:((z*400)-1)+200]

    # tokenize question and chunk
    input_ids = []
    question_tokens = tokenizer.encode(question)
    input_ids = question_tokens + chunk

    # create tokens list for each for later
    tokens = tokenizer.convert_ids_to_tokens(input_ids)

    # calculate number of tokens in segment A
    num_seg_a = input_ids.index(tokenizer.sep_token_id) + 1

    # calculate number of tokens in segment B
    num_seg_b = len(input_ids) - num_seg_a

    # create segment IDs
    segment_ids = [0] * num_seg_a + [1] * num_seg_b

    # check that segment IDs are correct length
    assert len(segment_ids) == len(input_ids)

    # run model
    start_scores, end_scores = model(torch.tensor([input_ids]),  # The tokens representing our input text.
                                     token_type_ids=torch.tensor([segment_ids]),
                                     return_dict=False)  # The segment IDs to differentiate question from answer_text

    # find start and end indices of answer
    answer_start = torch.argmax(start_scores)
    answer_end = torch.argmax(end_scores)

    # find start and end values of answer
    answer_start_value = torch.max(start_scores)

    answer_end_value = torch.max(end_scores)

    # calculate average value of answer
    average_value = (answer_start_value + answer_end_value) / 2

    if answer_start < len(tokens):
        answer = tokens[answer_start]
        for i in range(answer_start + 1, answer_end + 1):

            # If it's a subword token, then recombine it with the previous token.
            if tokens[i][0:2] == '##':
                answer += tokens[i][2:]

            # Otherwise, add a space then the token.
            else:
                answer += ' ' + tokens[i]

    else:
        answer = ""


    return answer, average_value
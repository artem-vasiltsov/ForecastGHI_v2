def estimate_data_accuracy(data):

    accuracy = True
    for value in data.values():

        if value == 0:
            accuracy = False
            break

    return accuracy


if __name__ == '__main__':

    estimate_data_accuracy(data={})

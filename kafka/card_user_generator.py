import random

def client_card_generator(n = 850):
    client_card = [[i, 1000+i, 
                   [round(random.uniform(49, 54), 2), round(random.uniform(14, 24), 2)], 
                    round(random.uniform(0, 100000), 2)] for i in range(1, n+1)]

    j = n + 1001
    for i in range(0, 800, 8):
        client_card.append([i+1, j, [random.randint(49, 54), random.randint(14, 24)],
                            round(random.uniform(0, 100000), 2)])
        j += 1

        if(i%16==0):
            client_card.append([i+1, j, [random.randint(49, 54), random.randint(14, 24)],
                                round(random.uniform(0, 100000), 2)])
            j += 1

    return client_card
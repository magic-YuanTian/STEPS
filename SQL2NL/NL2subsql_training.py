#Importing library
import numpy as np
import tensorflow as tf
from keras.models import Model
from keras.layers import Input, LSTM, Dense
from keras.utils import *
from keras.initializers import *


import time, random

# Hyperparameters
# After importing the libraries, we will specify the value for the hyperparameters including the batch size for training, latent dimensionality for the encoding space and the number of samples to train on.

batch_size = 64
latent_dim = 256
num_samples = 2000

# Vectorize the data.
# The below lines of codes will perform the data vectorization where we will read the file containing the English and corresponding French sentences. In the vectorization process, the collection of text documents is converted into feature vectors.

input_texts = []
target_texts = []
input_chars = set()
target_chars = set()

with open('nl_sql_pair.txt', 'r', encoding='utf-8') as f:
    lines = f.read().split('\n')
for line in lines[: min(num_samples, len(lines) - 1)]:
    input_text, target_text = line.split('\t')
    target_text = '\t' + target_text + '\n'
    input_texts.append(input_text)
    target_texts.append(target_text)
    for char in input_text:
        if char not in input_chars:
            input_chars.add(char)
    for char in target_text:
        if char not in target_chars:
            target_chars.add(char)

input_chars = sorted(list(input_chars))
target_chars = sorted(list(target_chars))
num_encoder_tokens = len(input_chars)
num_decoder_tokens = len(target_chars)
max_encoder_seq_length = max([len(txt) for txt in input_texts])
max_decoder_seq_length = max([len(txt) for txt in target_texts])

#Print size
print('Number of samples:', len(input_texts))
print('Number of unique input tokens:', num_encoder_tokens)
print('Number of unique output tokens:', num_decoder_tokens)
print('Max sequence length for inputs:', max_encoder_seq_length)
print('Max sequence length for outputs:', max_decoder_seq_length)

# Define data for encoder and decoder
# After getting the data set with all features, we will define the input data encoder and decoder and the target data for the decoder.

input_token_id = dict([(char, i) for i, char in enumerate(input_chars)])
target_token_id = dict([(char, i) for i, char in enumerate(target_chars)])

encoder_in_data = np.zeros((len(input_texts), max_encoder_seq_length, num_encoder_tokens), dtype='float32')

decoder_in_data = np.zeros((len(input_texts), max_decoder_seq_length, num_decoder_tokens), dtype='float32')

decoder_target_data = np.zeros((len(input_texts), max_decoder_seq_length, num_decoder_tokens), dtype='float32')

for i, (input_text, target_text) in enumerate(zip(input_texts, target_texts)):
    for t, char in enumerate(input_text):
        encoder_in_data[i, t, input_token_id[char]] = 1.
    for t, char in enumerate(target_text):
        decoder_in_data[i, t, target_token_id[char]] = 1.
        if t > 0:
            decoder_target_data[i, t - 1, target_token_id[char]] = 1.


# Define and process the input sequence
# Below lines of codes will define the input sequence for the encoder defined above and process this sequence. After that, an initial state will be set up for the decoder using ‘encoder_states’.

encoder_inputs = Input(shape=(None, num_encoder_tokens))
encoder = LSTM(latent_dim, return_state=True)
encoder_outputs, state_h, state_c = encoder(encoder_inputs)
#We discard `encoder_outputs` and only keep the states.
encoder_states = [state_h, state_c]

#Using `encoder_states` set up the decoder as initial state.
decoder_inputs = Input(shape=(None, num_decoder_tokens))
decoder_lstm = LSTM(latent_dim, return_sequences=True, return_state=True)
decoder_outputs, _, _ = decoder_lstm(decoder_inputs, initial_state=encoder_states)
decoder_dense = Dense(num_decoder_tokens, activation='softmax')
decoder_outputs = decoder_dense(decoder_outputs)

# The below line of code will define the final model that will turn `encoder_in_data` & `decoder_in_data` into `decoder_target_data`.

#Final model
model = Model([encoder_inputs, decoder_inputs], decoder_outputs)

# After defining the final model, we will check it by its summary, data shape and a visualization.

#Model Summary
model.summary()

#Model data Shape
print("encoder_in_data shape:",encoder_in_data.shape)
print("decoder_in_data shape:",decoder_in_data.shape)
print("decoder_target_data shape:",decoder_target_data.shape)

#Visuaize the model
tf.keras.utils.plot_model(model,show_shapes=True)

# Once we are ready with the final model, we will compile and train the model. Here, the model will be trained in 50 epochs only. For more accuracy, you can perform this for more number of epochs.

#Compiling and training the model
model.compile(optimizer=Adam(lr=0.01, beta_1=0.9, beta_2=0.999, decay=0.001), loss='categorical_crossentropy')

model.fit([encoder_in_data, decoder_in_data], decoder_target_data, batch_size = batch_size, epochs=50, validation_split=0.2)
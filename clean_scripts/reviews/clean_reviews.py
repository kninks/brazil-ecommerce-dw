import re

def clean_line(text):
    # Remove emojis
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # Emoticons
        "\U0001F300-\U0001F5FF"  # Symbols & Pictographs
        "\U0001F680-\U0001F6FF"  # Transport & Map
        "\U0001F1E0-\U0001F1FF"  # Flags
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "]+", flags=re.UNICODE
    )
    text = emoji_pattern.sub('', text)

    # Remove quotes and control characters
    text = re.sub(r'["\r\n\t]', ' ', text)

    # Remove excessive whitespace
    return text.strip()

# Read and clean the file
input_file = 'customer_reviews.txt'
output_file = 'cleaned_reviews.txt'

with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', encoding='utf-8') as outfile:
    for line in infile:
        cleaned = clean_line(line)
        if cleaned:  # Skip empty lines
            outfile.write(cleaned + '\n')

print(f"✅ Cleaned file saved as: {output_file}")
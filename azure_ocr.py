import azure.ai.vision as sdk
import re
import time
from pdf2image import convert_from_path
from api_keys import azure_key, azure_endpoint
def GetTextRead(cv_client, img_path):
    """Function for applying OCR and calling openai for text analysis

        args:
        image_file: path to the image file to apply ocr on
        cv_client: Azure AI Vision resource client

        Returns:
        text, error
    """

    # Use Analyze image function to read text in an image
    analysis_options  = sdk.ImageAnalysisOptions()
    analysis_options.features = (
        # Specify the features to be extracted from the image
        sdk.ImageAnalysisFeature.TEXT
    ) 

    # Configure the Icelandic language for the OCR
    analysis_options.language = 'en' 
    
    # Get Image analysis
    image = sdk.VisionSource(img_path)
    image_analyzer = sdk.ImageAnalyzer(cv_client, image, analysis_options)
    result = image_analyzer.analyze()
    # print(result.text)

    if result.reason == sdk.ImageAnalysisResultReason.ANALYZED:
        # If text was found within the image
        if result.text is not None:
            # print('OCR Text \n')
            letter_per_pixel = 0.10#len(result.text.lines[0].content) / (result.text.lines[0].bounding_polygon[2] - result.text.lines[0].bounding_polygon[0] )
            line_coordinate = {}
            # Read the lines extracted by the ocr from the image
            for line in result.text.lines:
                # Extract the polygon for the line
                r = line.bounding_polygon
                bounding_polygon = ((r[0], r[1]),(r[2], r[3]),(r[4], r[5]),(r[6], r[7]))
                line_coordinate[(line.content,(r[0], r[1]))] =  r[1]   # Top Left y-coordinate 
                # print(" Line: '{}', Bounding Polygon: {}".format(line.content, bounding_polygon))

            # Sort the lines in ascending order with respect to their topleft y coordinate
            minimum_x = min(line_coordinate.keys(),key=lambda k: k[1][0])[1][0]
            sorted_lines = sorted(line_coordinate.keys(), key=lambda k: line_coordinate[k])
            previous_topleft_y = 0 # For storing the y value for topleft coordinate for same line analysis

            # Analyze those lines which are in reality on the same line but have different y value for the top left by a
            # small threshold
            for line in sorted_lines:
                # if the difference between the two consecutive lines is less than 10 pixels, consider them one
                if line_coordinate[line] - previous_topleft_y<= 15:
                    line_coordinate[line] = previous_topleft_y
                previous_topleft_y = line_coordinate[line]

            # Resort the line coordinates wrt to the updated values
            sorted_lines = sorted(line_coordinate.keys(), key=lambda k: line_coordinate[k])

            # Reinitialize
            previous_topleft_y = 0
            product_text = ''
            words_in_line = {}

            for line_content,q1 in sorted_lines:
                # Extract the line obj based on the sorted lines list
                index = list(filter(lambda i : result.text.lines[i].content == line_content, range(len(result.text.lines))))
                line_obj = result.text.lines[index[0]]

                if line_coordinate[(line_content,q1)] != previous_topleft_y:
                    # line has ended, sort the words on the previous line on the basis of their topleft x coordinate
                    # and dump the sorted words to the product text variable 
                    # Print the sorted keys
                    sorted_words = sorted(words_in_line.keys(), key=lambda k: words_in_line[k][0])
                    words_sofar = 0
                    tp_print = ''
                    lin_sofar = ''
                    for word in sorted_words:
                        x1,x2 = words_in_line[word]
                        spaces = x1  * letter_per_pixel - len(lin_sofar)
                        sp = ' '*int(spaces)
                        pr = ''
                        # if x1 - minimum_x < 150:
                        #     pr = '\n@@@@@@@@@@\n'
                        tp_print += word+' : ' + str(x1) + ' : ' + str(int(spaces) ) + ' | '
                        lin_sofar += pr + sp +  word+' '
                        prev_last_x = x2
                    # print(tp_print)
                    product_text+=lin_sofar+'\n'
                    words_in_line.clear()
                    # Top left x coordinateof the current line considered
                    previous_topleft_y = line_coordinate[(line_content,q1)]

                # Return each word detected in the image and the position bounding box around each word with the confidence level of each word
                for word in line_obj.words:
                    r = word.bounding_polygon
                    bounding_polygon = ((r[0], r[1]),(r[2], r[3]),(r[4], r[5]),(r[6], r[7]))
                    words_in_line[word.content] = [r[0],r[2]] # x-coordinate if the top left


            # If the loop ended without writing all the lines in product text
            if bool(words_in_line):
                # Dump the sorted words of the previous line in the text file
                sorted_words = sorted(words_in_line.keys(), key=lambda k: words_in_line[k])
                # Print the sorted keys
                lin_sofar = ''
                for word in sorted_words:
                    x1,x2 = words_in_line[word]
                    spaces = x1  * letter_per_pixel - len(lin_sofar)
                    spaces = 4 * round(spaces/4)
                    sp = ' '*int(spaces)
                    tp_print += word+' : ' + str(x1) + ' : ' + str(int(spaces) ) + ' | '
                    lin_sofar+= sp +  word+' '
                    prev_last_x = x2
                product_text+=lin_sofar+'\n'
                words_in_line.clear()

        else:
            print('No text in image')
            return '',  'No text in image'

    else:
        error_details = sdk.ImageAnalysisErrorDetails.from_result(result)
        error = "Error reason: {}".format(error_details.reason) + "| Error code: {}".format(error_details.error_code) + "| Error message: {}".format(error_details.message)
        print(error)
        return '', error
    
    return product_text+'\n\n', ''

def read_ocr(pdf_path,f):
    
    endpoint = azure_endpoint
    key = azure_key

    # Authenticate the Azure AI Vision client for OCR capabilities
    cv_client = sdk.VisionServiceOptions(endpoint, key)
    
    images = convert_from_path(pdf_path, dpi=300)

    txt = '' 
    pages = []

    # images is a list of PIL images; you can now process each image as needed
    for i, image in enumerate(images):
        # For example, save each image as a JPEG
        image_path = f'images/page_{i + 1}.jpg'
        image.save(image_path, 'JPEG')

        t, e = GetTextRead(cv_client,image_path)
        t = re.sub(r',', '', t)
        if t != '':
            txt += t
            pages.append(t)
        else:
            raise Exception(f'file {pdf_path}, page {i+1},text empty with error : {e}')
    
    with open(f'txts/{f}.txt',"w", encoding="utf-8") as fo:
        fo.write(txt)

    return txt, pages



if __name__=="__main__":
    # Tracking time for code 
    from finance_tool import *
    start_time = time.time()

    files = glob.glob("./pdfs/*")

    for fy in range(len(files)):
        fil = files[fy]
        f = fil[7:-4]
        text, pages = read_ocr(fil,f)
        print(text)
        break
        
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Elapsed Time: {elapsed_time} s")
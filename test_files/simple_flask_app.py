#!usr/bin/python3


from flask import (Flask,
                   jsonify,
                   request)

app = Flask(__name__)

@app.route("/invocations", methods=["POST"])
def echo():
    usr_msg = request.form.get('msg')
    return jsonify({"msg": usr_msg+'_new'})

if __name__ == "__main__":
    app.run(host="0.0.0.0",
            port=5000)


from flask import Blueprint, render_template

views_bp = Blueprint('views', __name__)

@views_bp.route('/')
def home():
    return render_template('auto.html')

@views_bp.route('/auto')
def text_page():
    return render_template('auto.html')

@views_bp.route('/manual')
def image_page():
    return render_template('manual.html')

@views_bp.route('/stock')
def stock_page():
    return render_template('stock.html')
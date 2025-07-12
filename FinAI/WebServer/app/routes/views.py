from flask import Blueprint, render_template

views_bp = Blueprint('views', __name__)

@views_bp.route('/')
def home():
    return render_template('home.html')

@views_bp.route('/stock')
def stock_page():
    return render_template('stock.html')

@views_bp.route('/future')
def future_page():
    return render_template('future.html')
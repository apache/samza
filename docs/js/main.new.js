/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/***************************************
 * GLOBAL FUNCTIONS
 **************************************/

// credit: http://www.javascriptkit.com/javatutors/touchevents2.shtml
function swipedetect(el, callback){
  
	var touchsurface = el,
	swipedir,
	startX,
	startY,
	distX,
	distY,
	threshold = 50, //required min distance traveled to be considered swipe
	restraint = 100, // maximum distance allowed at the same time in perpendicular direction
	allowedTime = 500, // maximum time allowed to travel that distance
	elapsedTime,
	startTime,
	handleswipe = callback || function(swipedir, event){}

	touchsurface.addEventListener('touchstart', function(e){
			var touchobj = e.changedTouches[0]
			swipedir = 'none'
			dist = 0
			startX = touchobj.pageX
			startY = touchobj.pageY
			startTime = new Date().getTime() // record time when finger first makes contact with surface
			e.preventDefault()
	}, false)

	touchsurface.addEventListener('touchmove', function(e){
			e.preventDefault() // prevent scrolling when inside DIV
	}, false)

	touchsurface.addEventListener('touchend', function(e){
			var touchobj = e.changedTouches[0]
			distX = touchobj.pageX - startX // get horizontal dist traveled by finger while in contact with surface
			distY = touchobj.pageY - startY // get vertical dist traveled by finger while in contact with surface
			elapsedTime = new Date().getTime() - startTime // get time elapsed
			if (elapsedTime <= allowedTime){ // first condition for awipe met
					if (Math.abs(distX) >= threshold && Math.abs(distY) <= restraint){ // 2nd condition for horizontal swipe met
							swipedir = (distX < 0)? 'left' : 'right' // if dist traveled is negative, it indicates left swipe
					}
					else if (Math.abs(distY) >= threshold && Math.abs(distX) <= restraint){ // 2nd condition for vertical swipe met
							swipedir = (distY < 0)? 'up' : 'down' // if dist traveled is negative, it indicates up swipe
					}
			}
			handleswipe(swipedir, e)
			e.preventDefault()
	}, false)
}

/***************************************
 * MAIN MENU - NAVIGATION TOGGLE MOBILE
 **************************************/

const menu = document.querySelectorAll('[data-plugin="menu"]');
const menuOpened = document.querySelectorAll('[data-menu-opened]');
const menuClosed = document.querySelectorAll('[data-menu-closed]');

if (menu) {
	menuClosed.forEach( (closedItem) => {
		closedItem.addEventListener("click", function(e) {

			if (window.innerWidth > 768) {
				return;
			}

			menuClosed.forEach( (item) => {
				item.style.display = 'none';
			})
			menuOpened.forEach( (item) => {
				item.style.display = 'flex';
			})
		});
	});

	menuOpened.forEach( (openedItem) => {
		openedItem.addEventListener("click", function(e) {

			if (window.innerWidth > 768) {
				return;
			}

			menuOpened.forEach( (item) => {
				item.style.display = 'none';
			})
			menuClosed.forEach( (item) => {
				item.style.display = 'flex';
			})
		});
	});

	window.addEventListener("resize", function(e){
		if (window.innerWidth > 768) {
			menuOpened.forEach( (item) => {
				item.style = ''
			})
			menuClosed.forEach( (item) => {
				item.style = ''
			})
		}
	});
}

/***************************************
 * SIDE MENU - & RETRIEVE DOCUMENTATION DYNAMICALLY
 **************************************/

///////////////////////////
//
// SIDE MENU FUNCTIONS
//
///////////////////////////
 
var getDocumentationMenu = (url, cb, docMenu) => {
	var myRequest = new Request(url);
	
	fetch(myRequest).then((response) => {
		
		return response.text().then((text) => {
			cb(response.status, text, docMenu);
		});

	});
};

// This attaches all listeners
var doMenu = () => {
	const subMenuSelector = '[data-plugin="sub-menu"]';
	const subMenus = document.querySelectorAll(subMenuSelector);
	const topMenus = document.querySelectorAll('[data-plugin="top-menu"]');
	const curLoc = window.location.pathname;

	subMenus.forEach( (subMenu) => {

		var showClass = subMenu.getAttribute('data-sub-menu-show-class');

		// figure out opening the submenus and making things active
		var allSubSubMenus = subMenu.querySelectorAll('[data-sub-menu]');
		var openSubMenu = false;


		allSubSubMenus.forEach( (curSubSubMenu) => {

			var curSubMenuItems = curSubSubMenu.children;

			Array.from(curSubMenuItems).forEach( (child) => {
				var childLoc = child.getAttribute('href');
				var matchType = child.getAttribute('data-match-active');

				if (curLoc == childLoc && matchType == 'exact') {
					child.classList.add('active');
					openSubMenu = true;
					curSubSubMenu.classList.add(showClass);
				}

				if (curLoc.includes(childLoc) && matchType != 'exact') {
					child.classList.add('active');
					openSubMenu = true;
					curSubSubMenu.classList.add(showClass);
				}
			});

		});

		if (openSubMenu) {
			subMenu.classList.add(showClass);
		}

		// create the listener
		subMenu.addEventListener('click', function(e) {
			e.stopPropagation();

			// must be a title to trigger things:
			if (!e.target.classList.contains('side-navigation__group-title') && !e.target.classList.contains('side-navigation__group-title-icon')) {
				return;
			}

			var closestSubMenu = e.target.closest(subMenuSelector);

			closestSubMenu.classList.toggle(showClass);

			var siblings = Array.from(closestSubMenu.parentNode.children);

			siblings.forEach( sibling => {

				// Turn off all siblings (not itself)
				if (!sibling.isEqualNode(closestSubMenu)) {
					sibling.classList.remove(showClass);
				}

			});
			
		}, true);
	});

	topMenus.forEach( (topMenu) => {

		var loc = topMenu.getAttribute('href');
		var matchType = topMenu.getAttribute('data-match-active');

		if (curLoc == loc && matchType == 'exact') {
			topMenu.classList.add('active');
		}

		if (curLoc.includes(loc) && matchType != 'exact') {
			topMenu.classList.add('active');
		}

	});
};

// This takes the response of the documentation and builds the menu in right format
var buildDocMenu = (status, body, docMenu) => {
	if (status == 404) {
		// doMenu();
		// MONKEY

		var fallback = '/learn/documentation/versioned/';
		const docMenuSelector = '[data-documentation]';
		const docMenu = document.querySelector(docMenuSelector);

		// Already tried the fallback...
		if (docMenu && docMenu.getAttribute('data-documentation') == fallback) {
			doMenu();
			return;
		}

		// Trying the fallback
		docMenu.setAttribute('data-documentation', '/learn/documentation/versioned/');
		doSideMenu();
		return;
	}

	const docMenuLinkBase = docMenu.getAttribute('data-documentation');
	var div = document.createElement('div');
	div.innerHTML = body;

	div.querySelectorAll('h4').forEach( h4 => {

		var listItems = h4.nextElementSibling.children;


		var h4Link = h4.querySelector('a');

		if ((!listItems || !listItems.length) && !h4Link) {
			
			if (h4.textContent && h4.textContent.trim().length) {
				var h4Title = document.createElement('h4');
				h4Title.classList.add('side-navigation__group-item', 'title-no-link');
				h4Title.textContent = h4.textContent.trim();

				docMenu.appendChild(h4Title);
			} else {
			}

		} else if (h4Link) {
			var h4LinkEl = document.createElement('a');
			var h4href =h4Link.getAttribute('href');
			var linkDestination = h4href.match(/^http/) ? h4href : docMenuLinkBase + h4href;
			var linkText = h4Link.text.trim();

			h4LinkEl.classList.add('side-navigation__group-item');
			h4LinkEl.setAttribute('href', linkDestination);
			h4LinkEl.setAttribute('data-match-active', h4Link.getAttribute('data-match-active'));

			if (h4href.match(/^http/)) {
				h4LinkEl.setAttribute('target', '_blank');
				h4LinkEl.setAttribute('rel', 'nofollow');
			}

			h4LinkEl.text = linkText;
			
			docMenu.appendChild(h4LinkEl);

		} else {
			var group = document.createElement('div');
			var itemsDiv = document.createElement('div');
			var icon = document.createElement('i');
			icon.classList.add('side-navigation__group-title-icon', 'icon', 'ion-md-arrow-dropdown');
			group.classList.add('side-navigation__group', 'side-navigation__group--has-nested');
			group.setAttribute('data-sub-menu-show-class', 'side-navigation__group--has-nested-visible');
			group.setAttribute('data-plugin', 'sub-menu');
			itemsDiv.classList.add('side-navigation__group-items');
			itemsDiv.setAttribute('data-sub-menu', true);

			h4.classList.add('side-navigation__group-title');
			h4.prepend(icon);

			Array.from(listItems).forEach( listItem => {
				var link = listItem.querySelector('a');
				var linkhref = link.getAttribute('href');
				linkDestination = linkhref.match(/^http/) ? linkhref : docMenuLinkBase + linkhref;

				linkText = link.text.trim();

				var newLink = document.createElement('a');
				newLink.classList.add('side-navigation__group-item');
				newLink.setAttribute('href', linkDestination);
				newLink.text = linkText;

				if (linkhref.match(/^http/)) {
					newLink.setAttribute('target', '_blank');
					newLink.setAttribute('rel', 'nofollow');
				}

				itemsDiv.appendChild(newLink);
			})

			group.appendChild(h4);
			group.appendChild(itemsDiv);

			docMenu.appendChild(group);
		}

		if (h4.nextElementSibling && h4.nextElementSibling.tagName == 'HR') {
			docMenu.appendChild(document.createElement('hr'));
		}
	});

	// this needs to be called here bc this is the callback passed to asynchonous function getDocumentation..
	doMenu();
};

// This is the function to trigger to begin side menu setup.
var doSideMenu = () => {
	const docMenuSelector = '[data-documentation]';
	const docMenu = document.querySelector(docMenuSelector);

	// handle the mobile triggering of the side menu.
	handleMobileSideNavigation();

	if (docMenu) {
		const docMenuLinkBase = docMenu.getAttribute('data-documentation');
		const docMenuLink = window.location.protocol + '//' + window.location.host + docMenuLinkBase;
		// get the documentation menu, then build it.
		getDocumentationMenu(docMenuLink, buildDocMenu, docMenu);

	}
};

var handleMobileSideNavigation = function() {
	const containerToggle = document.querySelector('.container__toggle');
	const container = document.querySelector('.container');

	if (containerToggle) {
		// Detect clicks
		containerToggle.addEventListener("click", function(e){
			container.classList.toggle('container--opened');

			localStorage['container-opened'] = JSON.stringify(container.classList.contains('container--opened'));
		});

		if (localStorage['container-opened']) {

			var opened = JSON.parse(localStorage['container-opened']);
			
			if (opened) {
				container.classList.add('container--opened');
			} else {
				container.classList.remove('container--opened');
			} 
			
		} else {
			
			container.classList.remove('container--opened');
		}

		// Detect swipes
		swipedetect(containerToggle, function(swipedir) {
				// swipedir contains either "none", "left", "right", "top", or "down"
				if (!['left', 'right'].includes(swipedir)) {
						container.classList.toggle('container--opened');
						localStorage['container-opened'] = JSON.stringify(container.classList.contains('container--opened'));
						return;
				}

				// close it
				if (swipedir == 'left') {
					container.classList.add('container--opened');
				} else {
					container.classList.remove('container--opened');
				}

				localStorage['container-opened'] = JSON.stringify(container.classList.contains('container--opened'));
		});
	}
};

// SETUP
doSideMenu();


/***************************************
 * RELEASES LIST
 **************************************/
var doReleasesList = () => {
	const releasesListSelector = '[data-releases-list]';
	const releasesList = document.querySelector(releasesListSelector);
	const curLoc = window.location.pathname;

	var tryFile = function (url, cb) {
		var myRequest = new Request(url);
		fetch(myRequest).then((response) => {
			cb(response.status != 404);
		});
	}

	if (releasesList) {

		var items = releasesList.children;

		Array.from(items).forEach( item => {
			var anchor = item.querySelector('a');
			var link = anchor.getAttribute('href');

			tryFile(link, (status) => {

				if (status) {
					item.classList.remove('hide');
				} else {
					item.classList.add('hide');
				}

				if (curLoc.includes(link)) {
					anchor.classList.add('active');
				}
			})
		});

	}

};

doReleasesList();

/***************************************
 * CANVAS - HOME PAGE HERO
 **************************************/
// CREDIT: https://codepen.io/dudleystorey/pen/NbNjjX
var doCanvas = function() {
	const hero = document.getElementById("hero");
	const canvasBody = document.getElementById("canvas");

	if (canvasBody) {

		let resizeReset = function() {
			w = canvasBody.width = hero.offsetWidth;
			h = canvasBody.height = hero.offsetHeight;
		}

		const opts = { 
			particleColor: "rgb(200,200,200)",
			lineColor: "rgb(200,200,200)",
			particleAmount: 30,
			defaultSpeed: 1,
			variantSpeed: 1,
			defaultRadius: 2,
			variantRadius: 2,
			linkRadius: 200,
		};

		let deBouncer = function() {
				clearTimeout(tid);
				tid = setTimeout(function() {
						resizeReset();
				}, delay);
		};

		let checkDistance = function(x1, y1, x2, y2){ 
			return Math.sqrt(Math.pow(x2 - x1, 2) + Math.pow(y2 - y1, 2));
		};

		let linkPoints = function(point1, hubs){ 
			for (let i = 0; i < hubs.length; i++) {
				let distance = checkDistance(point1.x, point1.y, hubs[i].x, hubs[i].y);
				let opacity = 1 - distance / opts.linkRadius;
				if (opacity > 0) { 
					drawArea.lineWidth = 0.5;
					drawArea.strokeStyle = `rgba(${rgb[0]}, ${rgb[1]}, ${rgb[2]}, ${opacity})`;
					drawArea.beginPath();
					drawArea.moveTo(point1.x, point1.y);
					drawArea.lineTo(hubs[i].x, hubs[i].y);
					drawArea.closePath();
					drawArea.stroke();
				}
			}
		}

		Particle = function(xPos, yPos){ 
			this.x = Math.random() * w; 
			this.y = Math.random() * h;
			this.speed = opts.defaultSpeed + Math.random() * opts.variantSpeed; 
			this.directionAngle = Math.floor(Math.random() * 360); 
			this.color = opts.particleColor;
			this.radius = opts.defaultRadius + Math.random() * opts. variantRadius; 
			this.vector = {
				x: Math.cos(this.directionAngle) * this.speed,
				y: Math.sin(this.directionAngle) * this.speed
			};
			this.update = function(){ 
				this.border(); 
				this.x += this.vector.x; 
				this.y += this.vector.y; 
			};
			this.border = function(){ 
				if (this.x >= w || this.x <= 0) { 
					this.vector.x *= -1;
				}
				if (this.y >= h || this.y <= 0) {
					this.vector.y *= -1;
				}
				if (this.x > w) this.x = w;
				if (this.y > h) this.y = h;
				if (this.x < 0) this.x = 0;
				if (this.y < 0) this.y = 0;	
			};
			this.draw = function(){ 
				drawArea.beginPath();
				drawArea.arc(this.x, this.y, this.radius, 0, Math.PI*2);
				drawArea.closePath();
				drawArea.fillStyle = this.color;
				drawArea.fill();
			};
		};

		function setup(){ 
			particles = [];
			resizeReset();
			for (let i = 0; i < opts.particleAmount; i++){
				particles.push( new Particle() );
			}
			window.requestAnimationFrame(loop);
		}

		function loop(){ 
			window.requestAnimationFrame(loop);
			drawArea.clearRect(0,0,w,h);
			for (let i = 0; i < particles.length; i++){
				particles[i].update();
				particles[i].draw();
			}
			for (let i = 0; i < particles.length; i++){
				linkPoints(particles[i], particles);
			}
		}

		window.addEventListener("resize", function(){
			deBouncer();
		});

		drawArea = canvasBody.getContext("2d");
		let delay = 200, tid,
		rgb = opts.lineColor.match(/\d+/g);
		resizeReset();
		setup();
	}
}

// Do the canvas
doCanvas();


/***************************************
 * EVENTS
 **************************************/

var doEvents = () => {
	const eventSelector = '[data-plugin="event"]';
	const events = document.querySelectorAll(eventSelector);
	
	if (!events) {
		return;
	}

	events.forEach( event => {

		var date = event.getAttribute('data-date');
		var upcomingClass = event.getAttribute('data-upcoming-class');

		var d = new Date(date);
		var timestamp = d.getTime();

		var c = new Date();
		var curstamp = c.getTime();

		if (timestamp >= curstamp) {
			event.classList.add(upcomingClass);
		}
	});

};

doEvents();


/***************************************
 * PAGINATE
 **************************************/

var doPagination = () => {
	const paginateSelector = '[data-plugin="paginate"]';
	const paginates = document.querySelectorAll(paginateSelector);
	
	if (!paginates) {
		return;
	}

	paginates.forEach( paginate => {

		var maxPosts = parseInt(paginate.getAttribute('data-max-posts'));

		var posts = paginate.children;
		var count = 0;
		var nextBtn = document.querySelector(paginate.getAttribute('data-next'));
		var prevBtn = document.querySelector(paginate.getAttribute('data-previous'));
		var pagination = document.querySelector(paginate.getAttribute('data-pagination'));

		// Initial page load, hide extra posts
		Array.from(posts).forEach( post => {
			if (count >= maxPosts) {
				post.classList.add('hide');
			}

			count++
		});

		// Initial page load, determine if next shows
		if (posts && posts.length && posts.length > maxPosts) {
			nextBtn.classList.remove('hide');
		} else {
			pagination.classList.add('hide');
		}

		// handle next
		nextBtn.addEventListener("click", function(e) {
			
			// Find index of last showing post
			var visiblePosts = Array.prototype.filter.call(posts, (post) => {
				return !Array.from(post.classList).includes('hide');
			});

			var lastPost = visiblePosts[visiblePosts.length - 1];
			var index = Array.from(posts).indexOf(lastPost);

			// Hide all + show up to max posts after index
			Array.from(posts).forEach( (post, i) => {
				post.classList.add('hide');

				// show next posts and prev button
				if (i > index && i <= (index + maxPosts)) {
					post.classList.remove('hide');
					prevBtn.classList.remove('hide');
				}
				
			});

			// determine if next button does not show up
			if (posts.length <= index + maxPosts + 1) { // 1 bc indexes start at 0
				nextBtn.classList.add('hide');
			}

			paginate.scrollIntoView();
		});

		// handle previous
		prevBtn.addEventListener("click", function(e) {

			// Find index of first showing post
			var visiblePosts = Array.prototype.filter.call(posts, (post) => {
				return !Array.from(post.classList).includes('hide');
			});

			var firstPost = visiblePosts[0];
			var index = Array.from(posts).indexOf(firstPost);

			// Hide all + show up to max posts before index
			Array.from(posts).forEach( (post, i) => {
				post.classList.add('hide');

				// show next posts and next button
				if (i < index && i >= index - maxPosts) {
					post.classList.remove('hide');
					nextBtn.classList.remove('hide');
				}
				
			});

			// determine if prev button does not show up
			if (index - maxPosts == 0) {
				prevBtn.classList.add('hide');
			}

			paginate.scrollIntoView();
			
		});


	});
};

doPagination();